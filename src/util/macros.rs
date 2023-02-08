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
    use super::super::CloneableTuple;

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
    ($target:literal, $e:expr) => {{
        match $e {
            Ok(x) => x,
            Err(e) => {
                tracing::error!(target: $target, "{e}");
                break;
            }
        }
    }};
    ($target:literal, $e:expr, $($arg:tt)*) => {{
        match $e {
            Ok(x) => x,
            Err(e) => {
                tracing::error!(target: $target, error = ?e, $($arg)*);
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

// TODO: implement retry machenism
/// Macro to handle error with continue
macro_rules! ok_or_continue {
    ($target:literal, $e:expr) => {{
        match $e {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(target: $target, error = ?e, "Error, continue");
                continue;
            }
        }
    }};
    ($target:literal, $e:expr, $($arg:tt)*) => {{
        match $e {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(target: $target, error = ?e, $($arg)*, "Error, continue");
                continue;
            }
        }
    }};
}

macro_rules! ok_or_warn {
    ($target:literal, $e:expr) => {{
        if let Err(e) = $e {
            tracing::warn!(target: $target, error = ?e);
        }
    }};
    ($target:literal, $e:expr, $($arg:tt)*) => {{
        if let Err(e) = $e {
            tracing::warn!(target: $target, error = ?e, $($arg)*);
        }
    }};
}

pub(crate) use ok_or_break;
pub(crate) use ok_or_continue;
pub(crate) use ok_or_return;
pub(crate) use ok_or_warn;
