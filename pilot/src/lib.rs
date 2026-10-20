//! Leader-elected command, task, and task-event consumer for rayflow.
//!
//! Every pilot competes for a MySQL-backed leader lease. Only the holder
//! consumes the global command, task, and task-event logs; the others stand by
//! and take over when the lease expires or is released.

#![warn(missing_docs)]

mod config;
mod pilot;

pub use config::Config;
pub use pilot::{Error, Pilot, Step};
