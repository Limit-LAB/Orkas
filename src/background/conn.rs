//! Background connection management
//!
//! For now we will use pure rust TCP connection for transporting the messages.
//! In the future we will switch to QUIC or other protocol with Elixir for
//! better performance, scalability and security.

use std::{
    collections::{hash_map, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use color_eyre::Result;
use futures::{stream::SelectAll, SinkExt, StreamExt};
use kanal::AsyncReceiver;
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    select,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    adapt, ok_or_continue, AdaptedSink, AdaptedStream, CRDTMessage, MessageSet, MessageType,
    SWIMMessage, SendTo,
};

pub const DEFAULT_CHANNEL_SIZE: usize = 1 << 4;

type Inbound = AdaptedStream<OwnedReadHalf, MessageSet>;
type Outbound = AdaptedSink<OwnedWriteHalf, MessageSet>;

/// Entrance to background tasks. Contains all the channel sender.
#[derive(Clone, Debug)]
pub struct Entrance {
    pub msg: kanal::AsyncSender<SendTo>,
    pub crdt: kanal::AsyncSender<CRDTMessage>,
    pub swim: kanal::AsyncSender<SWIMMessage>,
    pub inbound: kanal::AsyncSender<Inbound>,
    pub outbound: kanal::AsyncSender<(SocketAddr, Outbound)>,
}

#[derive(Debug)]
pub struct BackgroundHandle {
    inbound: JoinHandle<Result<()>>,
    outbound: JoinHandle<Result<()>>,
    listener: JoinHandle<Result<()>>,
    cancel_token: CancellationToken,
    entrance: Arc<Entrance>,
}

impl BackgroundHandle {
    pub fn is_running(&self) -> bool {
        !(self.cancel_token.is_cancelled()
            || self.inbound.is_finished()
            || self.outbound.is_finished()
            || self.listener.is_finished())
    }

    pub fn entrance(&self) -> &Entrance {
        &self.entrance
    }

    pub fn entrance_shared(&self) -> Arc<Entrance> {
        self.entrance.clone()
    }

    /// Issue a cancellation request to the background tasks.
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// Forcefully shutdown the background tasks with [`JoinHandle::abort`].
    ///
    /// [`JoinHandle::abort`]: tokio::task::JoinHandle::abort
    pub fn force_shutdown(self) {
        self.cancel();
        self.inbound.abort();
        self.outbound.abort();
        self.listener.abort();
    }

    /// Wait for the background tasks to finish. Use with
    /// [`BackgroundHandle::cancel`] to gracefully shutdown the background
    /// tasks.
    pub async fn join(self) -> Result<()> {
        self.inbound.await??;
        self.outbound.await??;
        self.listener.await??;
        Ok(())
    }
}

pub fn spawn_connections(
    bind: SocketAddr,
) -> (
    BackgroundHandle,
    AsyncReceiver<CRDTMessage>,
    AsyncReceiver<SWIMMessage>,
) {
    let (crdt, crdt_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (swim, swim_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (inbound, inbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (outbound, outbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (msg, msg_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);

    let entrance = Arc::new(Entrance {
        msg,
        crdt,
        swim,
        inbound,
        outbound,
    });

    let cancel_token = CancellationToken::new();

    let inbound = tokio::spawn(inbound_task(
        inbound_rx,
        entrance.clone(),
        cancel_token.clone(),
    ));
    let outbound = tokio::spawn(outbound_task(
        msg_rx,
        outbound_rx,
        entrance.clone(),
        cancel_token.clone(),
    ));
    let listener = tokio::spawn(listener_task(bind, entrance.clone(), cancel_token.clone()));
    let handle = BackgroundHandle {
        entrance,
        inbound,
        outbound,
        listener,
        cancel_token,
    };
    (handle, crdt_rx, swim_rx)
}

async fn inbound_task(
    recv: AsyncReceiver<Inbound>,
    ent: Arc<Entrance>,
    t: CancellationToken,
) -> Result<()> {
    let mut streams = SelectAll::<Inbound>::new();
    loop {
        if t.is_cancelled() {
            break;
        }
        select! {
            stream = recv.recv() => streams.push(stream?),
            msg = streams.next() => match msg {
                Some(Ok(msgs)) => {
                    for msg in msgs.message {
                        match msg {
                            MessageType::CRDT(msg) => ok_or_continue!(ent.crdt.send(msg).await),
                            MessageType::SWIM(msg) => ok_or_continue!(ent.swim.send(msg).await)
                        }
                    }
                },
                Some(Err(e)) => {
                    warn!("Error while reading inbound stream: {}", e);
                }
                None => {
                    // `SelectAll` is empty. Sleep for a while.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                },
            }
        }
    }
    Ok(())
}

async fn outbound_task(
    msg_recv: AsyncReceiver<SendTo>,
    conn_recv: AsyncReceiver<(SocketAddr, Outbound)>,
    ent: Arc<Entrance>,
    t: CancellationToken,
) -> Result<()> {
    let mut map = HashMap::with_capacity(1 << 4);

    loop {
        if t.is_cancelled() {
            break;
        }
        select! {
            msg = msg_recv.recv() => {
                let msg = msg?;
                if let hash_map::Entry::Vacant(e) = map.entry(msg.addr) {
                    let conn = TcpStream::connect(msg.addr)
                        .await
                        .map(|x| adapt::<_, _, MessageSet>(x.into_split()));
                    let (stream, sink) = ok_or_continue!(conn);
                    e.insert(sink);

                    // Continue even if inbound task is dropped
                    drop(ent.inbound.send(stream).await);
                };
                let conn = map.get_mut(&msg.addr).unwrap();
                ok_or_continue!(conn.send(msg.msg).await)
            },
            conn = conn_recv.recv() => {
                let (addr, conn) = ok_or_continue!(conn);
                map.insert(addr, conn);
            }
        }
    }
    Ok(())
}

async fn listener_task(bind: SocketAddr, ent: Arc<Entrance>, t: CancellationToken) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    loop {
        if t.is_cancelled() {
            break;
        }
        let (stream, addr) = ok_or_continue!(listener.accept().await);
        tracing::debug!("New connection from {}", addr);
        let (stream, sink) = adapt::<_, _, MessageSet>(stream.into_split());
        match (
            ent.inbound.send(stream).await,
            ent.outbound.send((addr, sink)).await,
        ) {
            (Ok(_), Ok(_)) => {}
            (Err(e), _) | (_, Err(e)) => tracing::error!("Error in listener: {}", e),
        }
    }
    Ok(())
}
