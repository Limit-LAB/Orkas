use std::{
    collections::{hash_map, HashMap},
    net::SocketAddr,
    sync::Arc,
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
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    adapt, ok_or_break, ok_or_continue, ok_or_return, AdaptedSink, AdaptedStream, CRDTMessage,
    MessageSet, MessageType, SWIMMessage,
};

pub const DEFAULT_CHANNEL_SIZE: usize = 1 << 4;

type Inbound = AdaptedStream<OwnedReadHalf, MessageSet>;
type Outbound = AdaptedSink<OwnedWriteHalf, MessageSet>;

struct Entrance {
    msg: kanal::AsyncSender<SendTo>,
    crdt: kanal::AsyncSender<CRDTMessage>,
    swim: kanal::AsyncSender<SWIMMessage>,
    inbound: kanal::AsyncSender<Inbound>,
    outbound: kanal::AsyncSender<(SocketAddr, Outbound)>,
}

async fn inbound_task(
    recv: AsyncReceiver<Inbound>,
    ent: Arc<Entrance>,
    t: CancellationToken,
) -> Result<()> {
    let mut streams = SelectAll::<Inbound>::new();
    loop {
        select! {
            // Quit if all senders are dropped
            stream = recv.recv() => streams.push(stream?),
            msg = streams.next() => match msg {
                Some(Ok(msgs)) => {
                    for msg in msgs.message {
                        match msg {
                            MessageType::CRDT(msg) => ok_or_break!(ent.crdt.send(msg).await),
                            MessageType::SWIM(msg) => ok_or_break!(ent.swim.send(msg).await)
                        }
                    }
                },
                Some(Err(e)) => {
                    warn!("Error in inbound stream: {}", e);
                }
                None => break,
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
) {
    let mut map = HashMap::with_capacity(1 << 4);

    loop {
        select! {
            msg = msg_recv.recv() => {
                let msg = ok_or_continue!(msg);
                if let hash_map::Entry::Vacant(e) = map.entry(msg.addr) {
                    let conn = TcpStream::connect(msg.addr)
                        .await
                        .map(|x| adapt::<_, _, MessageSet>(x.into_split()));
                    let (stream, sink) = ok_or_continue!(conn);
                    e.insert(sink);
                    ok_or_continue!(ent.inbound.send(stream).await);
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
}

async fn listener_task(bind: SocketAddr, ent: Arc<Entrance>, t: CancellationToken) {
    let listener = ok_or_return!(tokio::net::TcpListener::bind(bind).await);
    loop {
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
}

pub fn spawn_background(bind: SocketAddr) {
    let (crdt, crdt_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (swim, swim_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (inbound, inbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (outbound, outbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (msg, msg_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);

    let ent = Arc::new(Entrance {
        msg,
        crdt,
        swim,
        inbound,
        outbound,
    });

    let token = CancellationToken::new();

    tokio::spawn(inbound_task(inbound_rx, ent.clone(), token.clone()));
    tokio::spawn(outbound_task(
        msg_rx,
        outbound_rx,
        ent.clone(),
        token.clone(),
    ));
    tokio::spawn(listener_task(bind, ent, token));
}

struct SendTo {
    pub msg: MessageSet,
    pub addr: SocketAddr,
}
