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
use tokio::{net::TcpStream, select};
use tracing::{debug, info, warn};

use crate::{
    codec::adapt,
    model::{MessageType, SendTo},
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

        match select(pin!(recv.recv()), pin!(streams.next())).await {
            Either::Left((stream, _)) => streams.push(stream?),
            Either::Right((msgs, _)) => match msgs {
                Some(Ok(set)) => {
                    for msg in set.message {
                        let Message {
                            sender,
                            id,
                            topic,
                            body,
                        } = msg;
                        match body {
                            MessageType::CRDT(crdt) => {
                                ok_or_continue!(
                                    "inbound",
                                    ctx.crdt_inbound.send((topic, crdt)).await
                                )
                            }
                            MessageType::SWIM(swim) => {
                                if let Some(handle) = ctx.topics.get(&topic) {
                                    ok_or_continue!(
                                        "inbound",
                                        handle.swim.sender.send(swim.data).await
                                    );
                                } else {
                                    info!(
                                        target: "inbound",
                                        message_type = "swim",
                                        sender = %msg.sender,
                                        topic,
                                        "Non-exist topic, ignore",
                                    );
                                };
                            }
                            MessageType::Join { id, swim_data } => {
                                if let Some(handle) = ctx.topics.get(&topic) {
                                    todo!("handle join request")
                                } else {
                                    info!(
                                        target: "inbound",
                                        message_type = "join",
                                        sender = %msg.sender,
                                        topic,
                                        "Non-exist topic, ignore",
                                    );
                                };
                            }
                            MessageType::JoinResponse {
                                respond_to,
                                swim_data,
                                snapshot,
                            } => {
                                if let Some(mut handle) = ctx.topics.get_mut(&topic) {
                                    handle.logs = snapshot;
                                    handle.swim.sender.send(swim_data).await?;
                                } else {
                                    info!(
                                        target: "inbound",
                                        message_type = "join_response",
                                        sender = %msg.sender,
                                        topic,
                                        "Non-exist topic, ignore",
                                    );
                                };
                            }
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
            },
        }
    }
    Ok(())
}

/// Aggregate all outbound data and dispatch them to corresponding targets.
pub(super) async fn outbound_task(
    msg_recv: AsyncReceiver<SendTo>,
    conn_recv: AsyncReceiver<(SocketAddr, Outbound)>,
    ctx: Context,
) -> Result<()> {
    // TODO: maybe change this to LRU?
    let mut map = HashMap::with_capacity(1 << 4);

    loop {
        if ctx.cancel_token.is_cancelled() {
            break;
        }
        select! {
            msg = msg_recv.recv() => {
                let msg = msg?;
                // TODO: better retry
                let mut retry = 3;
                loop {
                    if retry == 0 {
                        warn!(target: "outbound", address = %msg.addr, "Failed to send message");
                        break;
                    }
                    if let hash_map::Entry::Vacant(entry) = map.entry(msg.addr) {
                        let conn = TcpStream::connect(msg.addr)
                            .await
                            .map(|s| adapt(s.into_split()));
                        let (stream, sink) = ok_or_continue!("outbound", conn);
                        entry.insert(sink);

                        // Continue even if inbound has stopped
                        drop(ctx.conn_inbound.send(stream).await);
                    };
                    let conn = map.get_mut(&msg.addr).unwrap();
                    match conn.send(msg.msg.clone()).await {
                        Ok(_) => break,
                        Err(e) => {
                            debug!(target: "outbound", error = %e, "Connection disconnected, retry");
                            map.remove(&msg.addr);
                        }
                    }
                    retry -= 1;
                }
            },
            conn = conn_recv.recv() => {
                let (addr, conn) = ok_or_continue!("outbound", conn);
                map.insert(addr, conn);
            }
        }
    }
    Ok(())
}

/// Accept income connections and send them to the inbound and outbound task
pub(super) async fn listener_task(bind: SocketAddr, ent: Context) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    loop {
        if ent.cancel_token.is_cancelled() {
            break;
        }
        let (stream, src) = ok_or_continue!("listener", listener.accept().await);
        tracing::trace!(target: "listener", %src, "New connection");
        let (stream, sink) = adapt(stream.into_split());
        match (
            ent.conn_inbound.send(stream).await,
            ent.conn_outbound.send((src, sink)).await,
        ) {
            (Ok(_), Ok(_)) => {}
            (Err(e), _) | (_, Err(e)) => tracing::error!("Error in listener: {}", e),
        }
    }
    Ok(())
}
