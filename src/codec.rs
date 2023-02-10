/// A framed codec that uses bincode to serialize and deserialize messages.
use std::{any::type_name, fmt::Debug, io::Cursor};

use bytes::{Buf, BufMut, BytesMut};
use color_eyre::{eyre::Context, Result};
use futures::{Sink, Stream};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tracing::{debug, instrument, trace, warn};

use crate::Envelope;

pub type MessageStream<R: AsyncRead> = impl Stream<Item = Result<Envelope>>;
pub type MessageSink<W: AsyncWrite> = impl Sink<Envelope, Error = color_eyre::eyre::Error>;

pub use bincode_option_mod::{bincode_option, BincodeOptions};

/// Workaround for rust resolving `BincodeOptions` to two different types
mod bincode_option_mod {
    use bincode::Options;

    pub type BincodeOptions = impl Options + Copy;

    #[inline(always)]
    pub fn bincode_option() -> BincodeOptions {
        bincode::DefaultOptions::new()
            .reject_trailing_bytes()
            .with_limit(1 << 8)
        // .with_varint_encoding()
    }
}

pub fn adapt<R, W>(stream: (R, W)) -> (MessageStream<R>, MessageSink<W>)
where
    R: AsyncRead,
    W: AsyncWrite,
{
    let (r, w) = stream;
    let codec = SerdeBincodeCodec::new();
    let stream = FramedRead::new(r, codec);
    let sink = FramedWrite::new(w, codec);
    (stream, sink)
}

pub fn adapt_with_option<R, W, T, O>(
    stream: (R, W),
    option: O,
) -> (
    impl Stream<Item = Result<T>>,
    impl Sink<T, Error = color_eyre::eyre::Error>,
)
where
    R: AsyncRead,
    W: AsyncWrite,
    T: Serialize + DeserializeOwned + Debug,
    O: bincode::Options + Clone,
{
    let (r, w) = stream;
    let codec = SerdeBincodeCodec::<T, O>::with_option(option);
    let stream = FramedRead::new(r, codec.clone());
    let sink = FramedWrite::new(w, codec);
    (stream, sink)
}

/// A codec that uses consecutive bincode to serialize and deserialize
/// messages.
#[must_use]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SerdeBincodeCodec<T, O> {
    option: O,
    _marker: std::marker::PhantomData<T>,
}

impl<T, O> Clone for SerdeBincodeCodec<T, O>
where
    O: Clone,
{
    fn clone(&self) -> Self {
        Self {
            option: self.option.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, O> Copy for SerdeBincodeCodec<T, O> where O: Copy {}

impl<T> SerdeBincodeCodec<T, BincodeOptions> {
    pub fn new() -> Self {
        Self {
            option: bincode_option(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, O> SerdeBincodeCodec<T, O> {
    pub fn with_option(option: O) -> Self {
        Self {
            option,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> Default for SerdeBincodeCodec<T, BincodeOptions> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Serialize, O: bincode::Options + Clone> Encoder<T> for SerdeBincodeCodec<T, O> {
    type Error = color_eyre::eyre::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> std::result::Result<(), Self::Error> {
        let b = self
            .option
            .clone()
            .serialize(&item)
            .wrap_err_with(|| format!("Failed to serialize `{}`", type_name::<T>()))?;

        dst.put_slice(&b);

        Ok(())
    }
}

impl<T: DeserializeOwned + Debug, O: bincode::Options + Clone> Decoder for SerdeBincodeCodec<T, O> {
    type Error = color_eyre::eyre::Error;
    type Item = T;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        try_decode(src, self.option.clone())
            .wrap_err_with(|| format!("Failed to deserialize `{}`", type_name::<T>()))
    }
}

/// Try to decode a message from the given buffer and update buffer's cursor if
/// bytes are filled. Otherwise, this will return a `Ok(None)` indicating that
/// the buffer is not filled yet and leave the buffer unchanged. However if
/// other errors happen, this will return a `Err` indicating that the buffer is
/// corrupted.
#[instrument(level = "trace", skip(data, option), fields(bytes = data.chunk().len()))]
pub fn try_decode<T: DeserializeOwned + Debug>(
    data: &mut impl Buf,
    option: impl bincode::Options,
) -> Result<Option<T>, bincode::Error> {
    if data.chunk().is_empty() {
        return Ok(None);
    }
    let mut cur = Cursor::new(data.chunk());

    let res = option.deserialize_from::<_, T>(&mut cur);

    trace!("Read {} bytes", cur.position());

    match res {
        Ok(val) => {
            data.advance(cur.position() as usize);
            debug!(?val, "Decoded");

            Ok(Some(val))
        }
        // Buffer is not filled (yet), not an error. Leave the cursor untouched so that
        // remaining bytes can be used in the next decode attempt.
        Err(e) => match *e {
            // Buffer is not filled (yet), not an error. Leave the cursor untouched so that
            // remaining bytes can be used in the next decode attempt.
            bincode::ErrorKind::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            _ => {
                warn!(error=?e, "Failed to deserialize message.");
                Err(e.into())
            }
        },
    }
}

#[test]
fn test_codec() {
    use serde::Deserialize;
    use tap::Pipe;
    use tracing::info;

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .try_init()
        .pipe(drop);

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    struct A {
        a: String,
        num: u32,
    }
    let a = A {
        a: "hello\n\n123".to_string(),
        num: 10,
    };

    let mut enc = SerdeBincodeCodec::<A, _>::new();
    let mut w = BytesMut::new();

    info!("Encoding");
    enc.encode(a.clone(), &mut w).unwrap();

    info!("{:#?}", &w[..]);
    info!("Decoding");
    let a2 = enc.decode(&mut w).unwrap();
    info!("{a2:#?}");

    // assert_eq!(a, a2);
}

#[tokio::test]
async fn test_framed() -> Result<()> {
    use futures::{SinkExt, StreamExt};
    use serde::Deserialize;
    use tap::Pipe;
    use tracing::info;

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .try_init()
        .pipe(drop);
    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    struct A {
        a: String,
        num: u32,
    }

    let a = A {
        a: "hello\n\n123".to_string(),
        num: 10,
    };
    let b = A {
        a: "hello".to_owned(),
        num: 114514,
    };

    let enc = SerdeBincodeCodec::<A, _>::new();
    let mut w = vec![];

    {
        let mut w = FramedWrite::new(&mut w, enc);
        w.send(a.clone()).await?;
        w.send(b.clone()).await?;
    }
    info!("Written: {w:?}");
    let mut r = FramedRead::new(&w[..], enc);

    assert_eq!(r.next().await.unwrap()?, a);
    assert_eq!(r.next().await.unwrap()?, b);
    assert!(r.next().await.is_none());

    Ok(())
}

#[test]
fn test_bincode_ser() {
    use bincode::Options;
    use uuid7::Uuid;

    #[derive(Debug, Serialize, PartialEq, Eq, Clone)]
    struct Meta {
        id: uuid7::Uuid,
        topic: String,
    }
    #[derive(Debug, Serialize, PartialEq, Eq, Clone)]
    struct Ser {
        meta: Meta,
        data: BytesMut,
    }

    let a = Ser {
        meta: Meta {
            id: Uuid::MAX,
            topic: "111".to_owned(),
        },
        data: BytesMut::from([1, 1, 0, 1, 1, 0].as_slice()),
    };

    let b = bincode_option().serialize(&a).unwrap();

    println!("Len {}", b.len());
    println!("{:?}", b);
}
