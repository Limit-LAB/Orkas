//! Background task of SWIM

use std::pin::pin;

use bincode::DefaultOptions;
use bytes::Bytes;
use color_eyre::Result;
use foca::{BincodeCodec, Foca, NoCustomBroadcast, Runtime};
use futures::future::{select, Either};
use kanal::AsyncSender;
use rand::rngs::StdRng;

use crate::{
    model::Id,
    tasks::{Context, DEFAULT_CHANNEL_SIZE},
    util::{ok_or_break, ok_or_continue, ok_or_warn},
    Envelope, InternalMessage,
};

#[allow(clippy::upper_case_acronyms)]
pub type SWIM = Foca<Id, BincodeCodec<DefaultOptions>, StdRng, NoCustomBroadcast>;

/// Shim runtime that implements [`foca::Runtime`] trait.
#[must_use]
pub(crate) struct SwimRuntime<'a> {
    topic: &'a str,
    ctx: &'a Context,
    msg_buf: Vec<Envelope>,
}

impl SwimRuntime<'_> {
    pub(crate) async fn flush(&mut self) -> Result<()> {
        for msg in std::mem::take(&mut self.msg_buf) {
            self.ctx.msg.send(msg).await?;
        }
        Ok(())
    }
}

impl Drop for SwimRuntime<'_> {
    fn drop(&mut self) {
        if self.msg_buf.is_empty() {
            return;
        }
        let send = self.ctx.msg.clone();
        let msgs = std::mem::take(&mut self.msg_buf);

        tokio::spawn(async move {
            for msg in msgs {
                send.send(msg).await.unwrap();
            }
        });
    }
}

impl<'a> Runtime<Id> for SwimRuntime<'a> {
    fn notify(&mut self, _notification: foca::Notification<Id>) {
        todo!("handle notification")
    }

    fn send_to(&mut self, to: Id, data: &[u8]) {
        self.msg_buf.push(Envelope {
            addr: to.addr(),
            topic: self.topic.to_owned(),
            body: data.to_vec().into(),
        })
    }

    fn submit_after(&mut self, event: foca::Timer<Id>, after: std::time::Duration) {
        let Some(topic) = self.ctx.topics.get(self.topic) else { return };
        let sender = topic.value().swim.internal_tx.clone();

        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            // Ignore if current target no longer running after sleep
            drop(sender.send(InternalMessage::Timer(event)).await);
        });
    }
}

#[derive(Debug, Clone)]
pub struct SwimJobHandle {
    internal_tx: AsyncSender<InternalMessage>,
    external_tx: AsyncSender<Bytes>,
}

impl SwimJobHandle {
    pub async fn send_internal(&self, msg: InternalMessage) -> Result<()> {
        self.internal_tx.send(msg).await?;
        Ok(())
    }

    pub async fn send_external(&self, msg: Bytes) -> Result<()> {
        self.external_tx.send(msg).await?;
        Ok(())
    }
}

impl Context {
    pub(crate) fn swim_runtime<'a>(&'a self, topic: &'a str) -> SwimRuntime<'a> {
        SwimRuntime {
            topic,
            ctx: self,
            msg_buf: Vec::new(),
        }
    }
}

pub(crate) async fn spawn_swim(
    topic: String,
    mut swim: SWIM,
    ctx: Context,
) -> Result<SwimJobHandle> {
    let (internal_tx, internal_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (external_tx, external_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    // let (sender, receiver) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);

    let handle = SwimJobHandle {
        internal_tx,
        external_tx,
    };

    tokio::spawn(async move {
        let mut rt = ctx.swim_runtime(&topic);
        loop {
            let (l, r) = (pin!(internal_rx.recv()), pin!(external_rx.recv()));

            let res = match select(l, r).await {
                Either::Left((internal, _)) => {
                    let msg = ok_or_break!("swim", internal, topic);
                    match msg {
                        InternalMessage::Timer(event) => swim.handle_timer(event, &mut rt),
                        InternalMessage::Broadcast(b) => {
                            todo!()
                        }
                    }
                }
                Either::Right((external, _)) => {
                    let msg = ok_or_break!("swim", external, topic);
                    swim.handle_data(&msg, &mut rt)
                }
            };
            ok_or_warn!("swim", res);
            ok_or_continue!("swim", rt.flush().await);
        }
    });

    Ok(handle)
}
