use std::{
    ops::{Deref, DerefMut, RangeBounds},
    time::SystemTime,
};

use tap::Pipe;
use uuid7::{uuid7, Uuid};

use crate::{util::GetTime, Log};

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct Events {
    events: Vec<Event>,
}

impl Events {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub fn push(&mut self, event: Event) {
        self.events.push(event);
    }

    pub fn drain(&mut self, range: impl RangeBounds<SystemTime>) -> Self {
        self.events
            .drain_filter(|v| range.contains(&v.get_time()))
            .collect::<Vec<_>>()
            .pipe(|x| Self { events: x })
    }

    pub fn earliest(&self) -> Option<&Event> {
        self.events.first()
    }

    pub fn into_inner(self) -> Vec<Event> {
        self.events
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.get_time().partial_cmp(&other.get_time())
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.get_time().cmp(&other.get_time())
    }
}

impl Deref for Events {
    type Target = Vec<Event>;

    fn deref(&self) -> &Self::Target {
        &self.events
    }
}

impl DerefMut for Events {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.events
    }
}

impl From<Vec<Event>> for Events {
    fn from(events: Vec<Event>) -> Self {
        Self { events }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub log: Log,
}

impl Event {
    pub fn new(log: Log) -> Self {
        Self { id: uuid7(), log }
    }
}
