// use std::{collections::BTreeMap, default::default, time::Duration};

// use uuid7::Uuid;

// use crate::{util::GetTime, Event};

// /// A buffer that holds events for a certain amount of time.
// ///
// /// ```text
// /// +--------------------------+--------------------------+
// /// | ^ Dead                   ^ Ready                    | <- New Items
// /// +--------------------------+--------------------------+
// /// |    Ready on next flush   |    On hold for sorting   |
// /// +--------------------------+--------------------------+
// /// ```
// #[derive(Debug, Clone)]
// pub struct WithholdEvent {
//     /// Events older than this will be sent on next flush.
//     window: Duration,

//     /// Events older than this will not be accepted.
//     deadline: Option<u64>,

//     /// Events that are on hold for sorting.
//     buffer: BTreeMap<Uuid, Event>,
// }

// impl WithholdEvent {
//     pub fn new(window: Duration) -> Self {
//         Self {
//             window,
//             deadline: default(),
//             buffer: BTreeMap::new(),
//         }
//     }

//     pub fn push(&mut self, item: Event) -> Option<Event> {
//         let time = item.id.get_ts();
//         let last = self.deadline.take().unwrap_or(time);

//         if time > last {
//             return Some(item);
//         }

//         self.buffer.insert(time, item);

//         None
//     }
// }
