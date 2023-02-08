use std::time::Duration;

use foca::Notification;
use tokio::{
    sync::oneshot::{channel, error::RecvError, Receiver, Sender},
    time::error::Elapsed,
};

use crate::model::Id;

#[derive(Debug, Clone)]
pub enum Event {
    Notification(Notification<Id>),
}
pub struct EventEmitter {
    sender: Sender<()>,
}

pub struct EventWaiter {
    recv: Receiver<()>,
}

#[test]
fn test_sync_send() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<EventEmitter>();
    assert_send_sync::<EventWaiter>();
}

impl EventEmitter {
    pub fn send(self) -> Result<(), ()> {
        self.sender.send(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WaitError {
    #[error("Receive error: {0}")]
    RecvError(#[from] RecvError),
    #[error("Timeout error: {0}")]
    Elapsed(#[from] Elapsed),
}

impl EventWaiter {
    pub async fn wait(self) -> Result<(), RecvError> {
        self.recv.await
    }

    pub async fn wait_timeout(self, timeout: Duration) -> Result<(), WaitError> {
        tokio::time::timeout(timeout, self.recv)
            .await?
            .map_err(Into::into)
    }
}

pub(crate) fn new_pair() -> (EventEmitter, EventWaiter) {
    let (sender, waiter) = channel();
    (EventEmitter { sender }, EventWaiter { recv: waiter })
}
