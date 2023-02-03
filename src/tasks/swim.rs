//! Background task of SWIM

use bincode::DefaultOptions;
use color_eyre::Result;
use foca::{BincodeCodec, Foca, NoCustomBroadcast, Runtime};
use kanal::{AsyncReceiver, AsyncSender};
use rand::rngs::StdRng;
use tracing::warn;
use uuid7::uuid7;

use crate::{
    model::{Id, SWIMMessage},
    tasks::Context,
    util::ok_or_continue,
    Message, MessageType,
};

#[allow(clippy::upper_case_acronyms)]
pub type SWIM = Foca<Id, BincodeCodec<DefaultOptions>, StdRng, NoCustomBroadcast>;

struct SwimRuntime<'a> {
    topic: &'a str,
    ctx: &'a Context,
    msgs: Vec<(Id, Message)>,
}

impl<'a> Runtime<Id> for SwimRuntime<'a> {
    fn notify(&mut self, notification: foca::Notification<Id>) {
        // TODO: handle notification
    }

    fn send_to(&mut self, to: Id, data: &[u8]) {
        self.msgs.push((
            to,
            Message {
                sender: to,
                id: uuid7(),
                topic: self.topic.to_owned(),
                body: MessageType::SWIM(SWIMMessage {
                    data: data.to_vec(),
                }),
            },
        ))
    }

    fn submit_after(&mut self, event: foca::Timer<Id>, after: std::time::Duration) {
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            // self.ctx.swim_sender.send(event).await.unwrap();
        });
    }
}

pub struct SwimJobHandle {
    pub sender: AsyncSender<Vec<u8>>,
}

impl Context {
    fn swim_runtime<'a>(&'a self, topic: &'a str) -> SwimRuntime<'a> {
        SwimRuntime {
            topic,
            ctx: self,
            msgs: Vec::new(),
        }
    }
}

pub(crate) async fn swim_timer(
    recv: AsyncReceiver<(String, foca::Timer<Id>)>,
    ctx: Context,
) -> Result<()> {
    loop {
        let (topic, timer) = ok_or_continue!("swim.timer", recv.recv().await);
        let Some(swim) = ctx.topics.get(&topic) else {
            warn!(target: "swim.timer", topic, "Non-exist topic, ignore");
            continue
        };
        let rt = ctx.swim_runtime(&topic);
    }
}

pub(crate) async fn spawn_swim(topic: String, swim: SWIM, ctx: Context) -> Result<SwimJobHandle> {
    todo!()
}
