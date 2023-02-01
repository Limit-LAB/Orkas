#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(is_sorted))]
#![feature(type_alias_impl_trait)]
#![feature(once_cell)]

use std::{net::SocketAddr, sync::Arc};

use bincode::DefaultOptions;
use color_eyre::Result;
use crossbeam_skiplist::SkipMap;
use foca::{BincodeCodec, Foca, NoCustomBroadcast};
// use quinn::{Endpoint, RecvStream, SendStream};
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
use tap::Pipe;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

mod_use::mod_use![config, codec, message, logs, data, util, conn];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Id {
    addr: SocketAddr,
    rev: u64, // TODO: use other id, like uuid
}

impl foca::Identity for Id {
    fn renew(&self) -> Option<Self> {
        let Self { addr, rev } = self;

        Some(Self {
            addr: *addr,
            rev: rev.wrapping_add(1),
        })
    }

    fn has_same_prefix(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl From<SocketAddr> for Id {
    fn from(addr: SocketAddr) -> Self {
        Self { addr, rev: 0 }
    }
}

type SendsMap = SkipMap<SocketAddr, AdaptedSink<OwnedWriteHalf, MessageSet>>;
type RecvsMap = SkipMap<SocketAddr, AdaptedStream<OwnedReadHalf, MessageSet>>;

pub struct Orkas {
    foca: Foca<Id, BincodeCodec<DefaultOptions>, ThreadRng, NoCustomBroadcast>,
    sends: Arc<SendsMap>,
    recvs: Arc<RecvsMap>,
    config: config::OrkasConfig,
}

impl Orkas {
    pub fn new(config: config::OrkasConfig) -> Result<Self> {
        let (server_conf, cert) = config.configure_server()?;
        let this = Self {
            foca: Foca::new(
                config.bind.into(),
                config.foca.clone(),
                ThreadRng::default(),
                BincodeCodec(DefaultOptions::new()),
            ),
            // tcp: Endpoint::server(server_conf, config.bind)?,
            sends: SkipMap::new().pipe(Arc::new),
            recvs: SkipMap::new().pipe(Arc::new),
            config,
        };

        Ok(this)
    }

    pub async fn main_loop(self) -> Result<()> {
        // let listen = self.tcp;
        let listen = TcpListener::bind(self.config.bind).await?;
        let cancel = CancellationToken::new();

        let s = self.sends.get(&self.config.bind.into()).unwrap();

        let listen_handle = cancellable_spawn(
            &cancel,
            (&self.sends, &self.recvs),
            |token, (sends, recvs)| async move {
                let listen = listen;
                loop {
                    if token.is_cancelled() {
                        break;
                    }
                    let (conn, addr) = listen.accept().await?;
                    // Try use 0-RTT. If failed, fallback to normalhandshake.                     //
                    // let conn = match conn.into_0rtt() {     Ok((conn, _)) =>
                    // conn,     Err(e) => e.await?,
                    // };
                    debug!("New connection from {addr}");

                    // adapt(conn);

                    // let (recv, send) = conn.into_split();

                    // sends.insert(addr, send);
                    // recvs.insert(addr, recv);
                }
                Ok(())
            },
        );

        let recv_handle =
            cancellable_spawn(&cancel, (&self.recvs,), |token, (recvs,)| async move {
                loop {
                    // for recv in recvs.iter() {
                    //     recv.value().
                    // }
                }
                Ok(())
            });
        Ok(())
    }
}

async fn read_ref(stream: &OwnedReadHalf, buf: &mut [u8]) -> std::io::Result<usize> {
    stream.readable().await?;
    stream.try_read(buf)
}

async fn write_ref(stream: &OwnedWriteHalf, buf: &[u8]) -> std::io::Result<usize> {
    stream.writable().await?;
    stream.try_write(buf)
}

struct Stream {
    stream: OwnedReadHalf,
}

// impl Stream {
//     fn new(stream: OwnedReadHalf) -> Self {
//         Self { stream }
//     }

//     // fn read<'a>(&'a self) -> StreamFut<'a> {
//     //     StreamFut {
//     //         fut: self.stream.readable(),
//     //         stream: &self.stream,
//     //     }
//     // }
// }

// impl AsyncRead for Stream {
//     fn poll_read(
//         mut self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//         buf: &mut [u8],
//     ) -> std::task::Poll<std::io::Result<usize>> {
//         self.stream.poll_peek(cx, &mut ReadBuf::new(buf))
//     }
// }

// type ReadableFut<'a> = impl Future<Output = std::io::Result<()>> + 'a;

// pin_project_lite::pin_project! {
//     struct StreamFut<'a> {
//         #[pin]
//         fut: ReadableFut<'a>,
//         stream: &'a OwnedReadHalf
//     }
// }

// impl Future for StreamFut<'_> {
//     type Output = std::io::Result<()>;

//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) ->
// Poll<Self::Output> {         let this = self.as_mut().project();
//         match this.fut.poll(cx) {
//             Poll::Ready(res) => {
//                 res?;
//                 let buf = &mut [0u8; 1024];
//                 self.stream.try_read();
//             }
//             Poll::Pending => todo!(),
//         }
//     }
// }
#[tokio::test]
async fn test_ref_tcp() {
    use bytes::BytesMut;
    use tokio::net::*;

    let (read, write) = TcpStream::connect("127.0.0.1:9090")
        .await
        .unwrap()
        .into_split();
    let mut buf = BytesMut::with_capacity(1024);

    write_ref(&write, "114514".as_bytes()).await.unwrap();
    let pos = 0;
    while let Ok(len) = read_ref(&read, &mut buf[..]).await {
        buf[pos..pos + len].fill(0);
    }
    println!("{buf:?}")
}
