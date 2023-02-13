use bincode::Options;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crdts::Dot;
use tap::{Pipe, TryConv};
use tracing::trace;
use uuid7::uuid7;

use crate::{
    codec::{bincode_option, try_decode},
    model::*,
    Actor,
};

impl Broadcast {
    pub fn new_crdt(op: LogOp) -> Self {
        Broadcast::CrdtOp(op)
    }

    pub fn new_events(events: Events) -> Self {
        Broadcast::Events(events)
    }

    /// Try to get the dot of the message for invalidation
    pub fn dot(&self) -> Option<Dot<Actor>> {
        match self {
            Broadcast::CrdtOp(op) => op.dot().pipe(Some),
            Broadcast::Events(_) => None,
        }
    }

    pub fn tag(&self) -> BroadcastTag {
        BroadcastTag::new(self.dot())
    }

    pub fn pack(&self) -> Result<BroadcastPack, bincode::Error> {
        let tag = self.tag();

        let opt = bincode_option();
        let len: usize = opt
            .serialized_size(&tag)?
            .try_conv::<usize>()
            .expect("Broadcast size too big")
            + opt
                .serialized_size(&self)?
                .try_conv::<usize>()
                .expect("Broadcast size too big");

        let mut buf = BytesMut::with_capacity(len + 8);
        buf.put_u64(len as _);
        let mut buf = buf.writer();

        opt.serialize_into(&mut buf, &tag)?;
        opt.serialize_into(&mut buf, &self)?;

        let data = buf.into_inner().freeze();
        trace!("Broadcast packed: {:?}", data);

        Ok(BroadcastPack { tag, data })
    }
}

impl From<LogOp> for Broadcast {
    fn from(op: LogOp) -> Self {
        Broadcast::CrdtOp(op)
    }
}

impl From<foca::Timer<Id>> for InternalMessage {
    fn from(timer: foca::Timer<Id>) -> Self {
        InternalMessage::Timer(timer)
    }
}

impl From<Broadcast> for InternalMessage {
    fn from(broadcast: Broadcast) -> Self {
        InternalMessage::Broadcast(broadcast)
    }
}

impl BroadcastPack {
    pub fn pack_with(buf: &mut BytesMut, bc: &Broadcast) -> Result<Self, bincode::Error> {
        Self::pack_with_tag(buf, bc.tag(), bc)
    }

    pub fn pack_with_tag(
        buf: &mut BytesMut,
        tag: BroadcastTag,
        bc: &Broadcast,
    ) -> Result<Self, bincode::Error> {
        let opt = bincode_option();
        let len: usize = opt
            .serialized_size(&tag)?
            .try_conv::<usize>()
            .expect("Broadcast size too big")
            + opt
                .serialized_size(&bc)?
                .try_conv::<usize>()
                .expect("Broadcast size too big");

        buf.reserve(len + 8);
        let mut buf = buf.split();
        buf.put_u64(len as _);
        let mut buf = buf.writer();

        opt.serialize_into(&mut buf, &tag)?;
        opt.serialize_into(&mut buf, &bc)?;

        let data = buf.into_inner().freeze();
        trace!("Broadcast packed: {:?}", data);

        Ok(BroadcastPack { tag, data })
    }

    pub fn pack(bc: &Broadcast) -> Result<Self, bincode::Error> {
        let mut buf = BytesMut::new();
        Self::pack_with(&mut buf, bc)
    }

    /// Unpack a [`BroadcastPack`] from a buffer and return its component. This
    /// will **not** modify the original buffer, include the cursor.
    pub fn unpack<B: Buf>(
        buf: &B,
    ) -> Result<Option<(u64, BroadcastTag, Broadcast)>, bincode::Error> {
        // Read the length without advancing the cursor
        let len = buf.chunk().get_u64();

        // Check if we have enough data to read the message
        if buf.remaining() < len as usize {
            return Ok(None);
        }

        // Offset by 8 bytes to skip the length
        let mut cur = &buf.chunk()[8..];

        let Some(tag) = try_decode::<BroadcastTag>(&mut cur, bincode_option())? else { return Ok(None) };
        let Some(bc) = try_decode::<Broadcast>(&mut cur, bincode_option())? else { return Ok(None) };

        Ok(Some((len, tag, bc)))
    }
}

impl BroadcastTag {
    pub fn new_dot(dot: Dot<Actor>) -> Self {
        Self {
            dot: Some(dot),
            id: uuid7(),
        }
    }

    pub fn new(dot: Option<Dot<Actor>>) -> Self {
        Self { dot, id: uuid7() }
    }

    pub(crate) fn pack_with(&self, data: Bytes) -> Result<BroadcastPack, bincode::Error> {
        Ok(BroadcastPack { tag: *self, data })
    }
}

impl AsRef<[u8]> for BroadcastPack {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[test]
fn test_serialize() {
    use std::{net::SocketAddr, str::FromStr};

    use crdts::{Dot, Identifier};
    use uuid7::uuid7;

    use crate::{model::LogOp, Actor, Log};

    let b = Broadcast::new_crdt(LogOp::Insert {
        id: Identifier::between(None, None, Dot::new(Actor::random(), 1).into()),
        val: Log::new("oops"),
    })
    .pack()
    .unwrap();

    let bb = bincode_option().serialize(&b).unwrap();
    let b2 = bincode_option().deserialize(&bb).unwrap();
    assert_eq!(b, b2);

    let m = Envelope {
        addr: SocketAddr::from_str("127.0.0.1:8000").unwrap(),
        topic: "test".to_owned(),
        body: Message::Swim(bb.into()),
        id: uuid7(),
    };

    let mb = bincode_option().serialize(&m).unwrap();
    let m2 = bincode_option().deserialize(&mb).unwrap();

    assert_eq!(m, m2);
}

#[test]
fn test_pack() {
    use crdts::Identifier;

    use crate::Log;

    let broadcast = Broadcast::new_crdt(LogOp::Insert {
        id: Identifier::between(None, None, Dot::new(Actor::random(), 1).into()),
        val: Log::new("oops"),
    });

    let pack = broadcast.pack().unwrap();
    let bytes = pack.data.as_ref();
    let (pos, tag, broadcast2) = BroadcastPack::unpack(&bytes).unwrap().unwrap();

    assert_eq!(bytes.len(), pos as usize + 8);
    assert_eq!(tag, pack.tag);
    assert_eq!(broadcast, broadcast2);
}
