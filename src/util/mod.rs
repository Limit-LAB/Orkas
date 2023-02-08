use std::{
    fmt::Debug,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use color_eyre::Result;
use futures::{task::AtomicWaker, Future};
use tokio::{
    task::JoinHandle,
    time::{error::Elapsed, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::model::{Actor, LogList, LogOp};

mod_use::mod_use![macros];

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

pub fn cancellable_spawn<F, Arg, Fut>(
    token: &CancellationToken,
    captures: Arg,
    func: F,
) -> JoinHandle<()>
where
    F: FnOnce(CancellationToken, Arg::Owned) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + Sync + 'static,
    Arg: CloneableTuple,
    Arg::Owned: Send + Sync + 'static,
{
    let token = token.clone();
    let arg = captures.clone_me();
    tokio::spawn(async move {
        if let Err(e) = func(token.clone(), arg).await {
            warn!("{e}");
            token.cancel();
        }
    })
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
