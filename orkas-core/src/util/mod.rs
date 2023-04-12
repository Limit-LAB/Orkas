use std::time::{Duration, SystemTime, UNIX_EPOCH};

use uuid7::Uuid;

use crate::model::{Actor, LogList, LogOp};

mod_use::mod_use![macros, retry];

pub trait CloneableTuple {
    type Owned;

    fn clone_me(self) -> Self::Owned;
}

impl CloneableTuple for () {
    type Owned = ();

    fn clone_me(self) -> Self::Owned {}
}

// TODO: Doc
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

// TODO: Doc
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

#[test]
fn test_uuid() {
    use uuid7::uuid7;

    let time = uuid7().get_time();

    let diff = SystemTime::now().duration_since(time).unwrap();

    assert!(diff.as_millis() < 2);
}
