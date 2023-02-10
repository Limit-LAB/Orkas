//! Background connection management
//!
//! For now we will use pure rust TCP connection for transporting the messages.
//! In the future we will switch to QUIC or other protocol with Elixir for
//! better performance, scalability and security.

use std::{
    collections::{hash_map, HashMap},
    net::SocketAddr,
    pin::pin,
    time::Duration,
};

use color_eyre::Result;
use futures::{
    future::{select, Either},
    stream::SelectAll,
    SinkExt, StreamExt,
};
use kanal::AsyncReceiver;
use tokio::{
    net::{TcpListener, TcpStream},
    select,
};
use tracing::{debug, info, trace, warn};
use uuid7::uuid7;

use crate::{
    codec::adapt,
    model::{Envelope, Topic},
    tasks::{Context, Inbound, Outbound},
    util::ok_or_continue,
    Message,
};

pub const DEFAULT_CHANNEL_SIZE: usize = 1 << 4;

/// Aggregate all inbound data and dispatch them to corresponding handler.
pub(super) async fn inbound_task(recv: AsyncReceiver<Inbound>, ctx: Context) -> Result<()> {
    let mut streams = SelectAll::<Inbound>::new();
    loop {
        if ctx.cancel_token.is_cancelled() {
            break;
        }

        let msg = select! {
            stream = recv.recv() => {
                streams.push(stream?);
                continue
            },
            msg = streams.next() => {
                msg
            }
            _ = ctx.cancel_token.cancelled() => break,
        };

        match msg {
            Some(Ok(msg)) => {
                let Envelope { topic, body, .. } = msg;
                let Some(handle) = ctx.topics.get(&topic) else {
                        info!(
                            target: "inbound",
                            message_type = "swim",
                            topic,
                            "Non-exist topic, ignore",
                        );
                        continue
                    };

                match body {
                    Message::Swim(bytes) => {
                        trace!(target: "inbound", message_type = "swim", ?bytes, "Received swim data");

                        ok_or_continue!("inbound", handle.value().swim.send_external(bytes).await);
                    }
                    Message::RequestSnapshot => {
                        trace!(target: "inbound", message_type = "request_snapshot", "Received request snapshot");

                        let topic = topic.clone();
                        let snapshot = handle.value().logs.clone();
                        let res = ctx
                            .msg
                            .send(Envelope {
                                addr: msg.addr,
                                topic,
                                body: Message::Snapshot { snapshot },
                                id: uuid7(),
                            })
                            .await;
                        ok_or_continue!("inbound", res)
                    }
                    Message::Snapshot { snapshot } => {
                        trace!(target: "inbound", message_type = "snapshot", "Received snapshot");

                        let swim = handle.value().swim.clone();
                        let new_topic = Topic {
                            logs: snapshot,
                            swim,
                        };
                        ctx.topics.insert(topic, new_topic);
                    }
                }
            }
            Some(Err(e)) => {
                warn!("Error while reading inbound stream: {}", e);
            }
            None => {
                // `SelectAll` is empty. Sleep for a while.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Ok(())
}

/// Aggregate all outbound data and dispatch them to corresponding targets.
pub(super) async fn outbound_task(
    msg_recv: AsyncReceiver<Envelope>,
    conn_recv: AsyncReceiver<(SocketAddr, Outbound)>,
    ctx: Context,
) -> Result<()> {
    // TODO: maybe change this to LRU?
    let mut map = HashMap::with_capacity(1 << 4);

    loop {
        if ctx.cancel_token.is_cancelled() {
            break;
        }

        let msg = select! {
            _ = ctx.cancel_token.cancelled() => break,
            msg = msg_recv.recv() => { msg? },
            conn = conn_recv.recv() => {
                let (addr, conn) = ok_or_continue!("outbound", conn);
                map.insert(addr, conn);
                continue
            }
        };

        let addr = msg.addr;

        trace!(target: "outbound", %addr, ?msg, "Sending message");

        // TODO: better retry
        let mut retry = 3;
        loop {
            if retry == 0 {
                warn!(target: "outbound", %addr, "Failed to send message");
                break;
            }
            if let hash_map::Entry::Vacant(entry) = map.entry(addr) {
                let conn = TcpStream::connect(addr)
                    .await
                    .map(|s| adapt(s.into_split()));
                let (stream, sink) = ok_or_continue!("outbound", conn);
                entry.insert(sink);

                // Continue even if inbound has stopped
                drop(ctx.conn_inbound.send(stream).await);
            };
            let conn = map.get_mut(&addr).unwrap();
            match conn.send(msg.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    debug!(target: "outbound", error = %e, "Send failed, retry");
                    map.remove(&addr);
                }
            }
            retry -= 1;
        }
    }
    Ok(())
}

/// Accept income connections and send them to the inbound and outbound task
pub(super) async fn listener_task(listener: TcpListener, ctx: Context) -> Result<()> {
    loop {
        let (stream, src) =
            match select(pin!(ctx.cancel_token.cancelled()), pin!(listener.accept())).await {
                Either::Left(_) => break,
                Either::Right((t, _)) => ok_or_continue!("listener", t),
            };
        tracing::trace!(target: "listener", %src, "New connection");
        let (stream, sink) = adapt(stream.into_split());
        match (
            ctx.conn_inbound.send(stream).await,
            ctx.conn_outbound.send((src, sink)).await,
        ) {
            (Ok(_), Ok(_)) => {}
            (Err(e), _) | (_, Err(e)) => tracing::error!("Error in listener: {}", e),
        }
    }
    Ok(())
}
