use std::{
    fmt::Debug,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use color_eyre::Result;
use futures::{task::AtomicWaker, Future};
use tokio::time::{error::Elapsed, timeout};
use uuid7::Uuid;

use crate::{
    model::{Actor, LogList, LogOp},
    Event,
};

mod_use::mod_use![macros, sorting_channel];

struct FlagInner {
    waker: AtomicWaker,
    flagged: AtomicBool,
}

/// A one-time flag thar can only be set to true and not otherwise.
#[derive(Clone)]
pub struct Flag(Arc<FlagInner>);

impl Debug for Flag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Flag")
            .field(&self.0.flagged.load(Ordering::Relaxed))
            .finish()
    }
}
impl Flag {
    pub fn new() -> Self {
        Self(Arc::new(FlagInner {
            waker: AtomicWaker::new(),
            flagged: AtomicBool::new(false),
        }))
    }

    pub fn notify(&self) {
        self.0.flagged.store(true, Ordering::Relaxed);
        self.0.waker.wake();
    }

    pub async fn timeout(self, duration: Duration) -> Result<(), Elapsed> {
        timeout(duration, self).await
    }
}

impl Default for Flag {
    fn default() -> Self {
        Self::new()
    }
}

impl Future for Flag {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // quick check to avoid registration if already done.
        if self.0.flagged.load(Ordering::Relaxed) {
            self.0.flagged.store(false, Ordering::Relaxed);
            return Poll::Ready(());
        }

        self.0.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.0.flagged.load(Ordering::Relaxed) {
            self.0.flagged.store(false, Ordering::Relaxed);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub trait CloneableTuple {
    type Owned;

    fn clone_me(self) -> Self::Owned;
}

impl CloneableTuple for () {
    type Owned = ();

    fn clone_me(self) -> Self::Owned {}
}

/// TODO: Doc
pub trait CRDTUpdater {
    type Error: std::error::Error;

    fn update(
        self,
        list: &LogList,
        actor: Actor,
    ) -> std::result::Result<Option<LogOp>, Self::Error>;
}

impl<E: std::error::Error, F: FnOnce(&LogList, Actor) -> std::result::Result<Option<LogOp>, E>>
    CRDTUpdater for F
{
    type Error = E;

    fn update(
        self,
        list: &LogList,
        actor: Actor,
    ) -> std::result::Result<Option<LogOp>, Self::Error> {
        self(list, actor)
    }
}

/// TODO: Doc
pub trait CRDTReader {
    type Return;

    fn read(self, topic: &LogList) -> Self::Return;
}

impl<T, F> CRDTReader for F
where
    F: FnOnce(&LogList) -> T,
{
    type Return = T;

    fn read(self, op: &LogList) -> T {
        self(op)
    }
}

pub trait GetTime {
    /// Retrive the time of the object
    fn get_time(&self) -> SystemTime;

    /// Retrive the timestamp of the object
    fn get_ts(&self) -> u64;
}

impl GetTime for Uuid {
    #[inline]
    fn get_time(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.get_ts())
    }

    #[inline]
    fn get_ts(&self) -> u64 {
        let mut bytes = [0; 8];
        bytes[2..].copy_from_slice(&self.as_bytes()[..6]);
        u64::from_be_bytes(bytes)
    }
}

impl GetTime for Event {
    #[inline]
    fn get_time(&self) -> SystemTime {
        self.id.get_time()
    }

    #[inline]
    fn get_ts(&self) -> u64 {
        self.id.get_ts()
    }
}

#[test]
fn test_uuid() {
    use uuid7::uuid7;

    let time = uuid7().get_time();

    let diff = SystemTime::now().duration_since(time).unwrap();

    // Process should be fast enough to not take more than 1ms
    assert_eq!(diff.as_millis(), 0);
}
