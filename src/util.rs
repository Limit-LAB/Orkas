use color_eyre::Result;
use futures::Future;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

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

macro_rules! impl_cloneable {
    ($($t:ident = $idx:tt),+) => {
        impl <$( $t ),+> CloneableTuple for ($( &$t, )+) where
            $( $t: Clone + Send + Sync + 'static, )+
        {
            type Owned = ( $( $t, )+);

            fn clone_me<'a>(self) -> Self::Owned {
                ( $( self.$idx.clone(), )+ )
            }
        }

    };
}

#[rustfmt::skip]
mod impls {
    use super::CloneableTuple;

    impl_cloneable!(T1 = 0);
    impl_cloneable!(T1 = 0, T2 = 1);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5, T7 = 6);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5, T7 = 6, T8 = 7);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5, T7 = 6, T8 = 7, T9 = 8);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5, T7 = 6, T8 = 7, T9 = 8, T10 = 9);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5, T7 = 6, T8 = 7, T9 = 8, T10 = 9, T11 = 10);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5, T7 = 6, T8 = 7, T9 = 8, T10 = 9, T11 = 10, T12 = 11);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5, T7 = 6, T8 = 7, T9 = 8, T10 = 9, T11 = 10, T12 = 11, T13 = 12);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5, T7 = 6, T8 = 7, T9 = 8, T10 = 9, T11 = 10, T12 = 11, T13 = 12, T14 = 13);
    impl_cloneable!(T1 = 0, T2 = 1, T3 = 2, T4 = 3, T5 = 4, T6 = 5, T7 = 6, T8 = 7, T9 = 8, T10 = 9, T11 = 10, T12 = 11, T13 = 12, T14 = 13, T15 = 14);
}

macro_rules! ok_or_break {
    ($e:expr) => {{
        match $e {
            Ok(x) => x,
            Err(e) => {
                tracing::error!("{e}");
                break;
            }
        }
    }};
    ($e:expr, $($arg:tt)*) => {{
        match $e {
            Ok(x) => x,
            Err(e) => {
                tracing::error!(error = %e, $($arg)*);
                break;
            }
        }
    }};
}

macro_rules! ok_or_return {
    ($e:expr) => {{
        match $e {
            Ok(x) => x,
            Err(e) => {
                tracing::error!("{e}");
                return;
            }
        }
    }};
}

// TODO: Implement retry machenism
macro_rules! ok_or_continue {
    ($e:expr) => {{
        match $e {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("{e}");
                continue;
            }
        }
    }};
}

pub(crate) use ok_or_break;
pub(crate) use ok_or_continue;
pub(crate) use ok_or_return;
