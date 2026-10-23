//! Shared global polling state for the append-only logs.

use std::time::Duration;

use jiff::Timestamp;
use toasty::Executor;

use crate::Error;

/// The `poll_offset` table: the global acknowledged offset of one append-only
/// log and the time at which its current gap was first observed.
#[derive(Debug, Clone, toasty::Model)]
#[table = "poll_offset"]
pub struct PollOffset {
    /// Log name: `command_entry`, `task`, or `task_event`.
    #[key]
    #[column(type = varchar(64))]
    pub name: String,
    /// Last offset whose row has been acknowledged.
    pub offset: u64,
    /// When the first unacknowledged gap was observed, if one is pending.
    pub gap_since: Option<Timestamp>,
    /// Last time the offset or gap state changed.
    pub updated_at: Timestamp,
}

/// Options shared by polling and acknowledgement of an append-only log.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// How long a missing auto-increment offset is held before it may be skipped.
    pub gap_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            gap_timeout: Duration::from_secs(10),
        }
    }
}

/// The persisted position of one log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Poll {
    /// Last row acknowledged by the consumer.
    pub offset: u64,
    /// When the current gap was first observed, if one is pending.
    pub gap_since: Option<Timestamp>,
}

/// The rows selected by one poll and the state to persist for them.
pub(crate) struct PollPlan<T> {
    /// Contiguous rows the caller may consume, in order.
    pub rows: Vec<T>,
    /// State to persist before returning the rows.
    pub state: Poll,
    /// Whether `state` differs from the state loaded for the poll.
    pub changed: bool,
}

/// Loads the persisted position of `name`, defaulting to the beginning of the
/// log when no row exists yet.
pub(crate) async fn load<E>(backend: &mut E, name: &str) -> Result<Poll, Error>
where
    E: Executor,
{
    let row = PollOffset::filter_by_name(name.to_owned())
        .first()
        .exec(&mut *backend)
        .await?;
    Ok(row.map_or_else(Poll::default, |row| Poll {
        offset: row.offset,
        gap_since: row.gap_since,
    }))
}

/// Selects the contiguous prefix of `rows` that can be consumed.
///
/// The first missing id starts a gap timer. Rows after that gap are not
/// returned until the timer reaches [`Config::gap_timeout`]. Once it does, the persisted
/// offset is safely advanced across the missing ids before the first returned
/// row, so a crash still leaves that row available to the next consumer.
pub(crate) fn plan<T>(
    rows: Vec<T>,
    state: Poll,
    now: Timestamp,
    id: impl Fn(&T) -> u64,
    config: Config,
) -> PollPlan<T> {
    let original = state;
    let mut persisted = state.offset;
    let mut expected = persisted.checked_add(1);
    let mut gap_since = state.gap_since;
    let mut selected = Vec::new();
    let mut stopped_at_gap = false;

    for row in rows {
        let row_id = id(&row);
        if row_id <= persisted {
            continue;
        }

        if expected == Some(row_id) {
            selected.push(row);
            expected = row_id.checked_add(1);
            continue;
        }

        let expired = gap_since.is_some_and(|since| gap_expired(now, since, config.gap_timeout));
        if selected.is_empty() && expired {
            // No row in this batch is waiting to be acknowledged, so the gap
            // can be crossed safely. The next poll starts at the first returned
            // row and still sees it if this consumer dies before acknowledging.
            if let Some(skip_to) = row_id.checked_sub(1) {
                persisted = skip_to;
                gap_since = None;
                selected.push(row);
                expected = row_id.checked_add(1);
                continue;
            }
        }

        if gap_since.is_none() {
            gap_since = Some(now);
        }
        stopped_at_gap = true;
        break;
    }

    if !stopped_at_gap {
        gap_since = None;
    }

    let state = Poll {
        offset: persisted,
        gap_since,
    };
    PollPlan {
        rows: selected,
        changed: state != original,
        state,
    }
}

/// Persists `state` for `name`, creating the row on first use.
pub(crate) async fn save<E>(backend: &mut E, name: &str, state: Poll) -> Result<(), Error>
where
    E: Executor,
{
    let now = Timestamp::now();
    match PollOffset::filter_by_name(name.to_owned())
        .first()
        .exec(&mut *backend)
        .await?
    {
        Some(row) => {
            PollOffset::filter_by_name(row.name)
                .update()
                .offset(state.offset)
                .gap_since(state.gap_since)
                .updated_at(now)
                .exec(&mut *backend)
                .await?;
        }
        None => {
            PollOffset::create()
                .name(name.to_owned())
                .offset(state.offset)
                .gap_since(state.gap_since)
                .updated_at(now)
                .exec(&mut *backend)
                .await?;
        }
    }
    Ok(())
}

/// Acknowledges `row_offset` for the log named `name`.
///
/// A contiguous acknowledgement preserves any pending gap timer; that timer
/// belongs to the id after the acknowledged prefix. A non-contiguous
/// acknowledgement is accepted only after the gap timeout, as a safety net for
/// callers that acknowledge outside the planner's order.
pub(crate) async fn ack<E>(
    backend: &mut E,
    name: &str,
    row_offset: u64,
    config: Config,
) -> Result<(), Error>
where
    E: Executor,
{
    let state = load(&mut *backend, name).await?;
    if row_offset <= state.offset {
        return Ok(());
    }

    let contiguous = state.offset.checked_add(1) == Some(row_offset);
    let skip_gap = !contiguous
        && state
            .gap_since
            .is_some_and(|since| gap_expired(Timestamp::now(), since, config.gap_timeout));
    if !contiguous && !skip_gap {
        return Ok(());
    }

    save(
        &mut *backend,
        name,
        Poll {
            offset: row_offset,
            gap_since: if contiguous { state.gap_since } else { None },
        },
    )
    .await
}

fn gap_expired(now: Timestamp, since: Timestamp, timeout: Duration) -> bool {
    since
        .checked_add(timeout)
        .is_ok_and(|deadline| now >= deadline)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(offset: u64, gap_since: Option<Timestamp>) -> Poll {
        Poll { offset, gap_since }
    }

    #[test]
    fn selects_the_contiguous_prefix_and_starts_a_gap_timer() {
        let now = Timestamp::now();
        let plan = plan(
            vec![1, 2, 4, 5],
            state(0, None),
            now,
            |row| *row,
            Config::default(),
        );

        assert_eq!(plan.rows, vec![1, 2]);
        assert_eq!(plan.state, state(0, Some(now)));
        assert!(plan.changed);
    }

    #[test]
    fn does_not_cross_a_fresh_gap() {
        let now = Timestamp::now();
        let since = now - Duration::from_secs(9);
        let plan = plan(
            vec![3, 4],
            state(1, Some(since)),
            now,
            |row| *row,
            Config::default(),
        );

        assert!(plan.rows.is_empty());
        assert_eq!(plan.state, state(1, Some(since)));
        assert!(!plan.changed);
    }

    #[test]
    fn crosses_an_expired_gap_without_acknowledging_the_returned_row() {
        let now = Timestamp::now();
        let since = now - Config::default().gap_timeout;
        let plan = plan(
            vec![3, 4],
            state(1, Some(since)),
            now,
            |row| *row,
            Config::default(),
        );

        assert_eq!(plan.rows, vec![3, 4]);
        assert_eq!(plan.state, state(2, None));
        assert!(plan.changed);
    }

    #[test]
    fn clears_an_old_gap_timer_after_contiguous_rows() {
        let now = Timestamp::now();
        let since = now - Duration::from_secs(1);
        let plan = plan(
            vec![2, 3],
            state(1, Some(since)),
            now,
            |row| *row,
            Config::default(),
        );

        assert_eq!(plan.rows, vec![2, 3]);
        assert_eq!(plan.state, state(1, None));
        assert!(plan.changed);
    }

    #[test]
    fn skips_already_acknowledged_rows() {
        let now = Timestamp::now();
        let plan = plan(
            vec![1, 2, 3, 5],
            state(2, None),
            now,
            |row| *row,
            Config::default(),
        );

        assert_eq!(plan.rows, vec![3]);
        assert_eq!(plan.state, state(2, Some(now)));
    }

    #[test]
    fn uses_the_configured_timeout_at_the_deadline() {
        let now = Timestamp::now();
        let since = now - Duration::from_millis(500);
        let state = state(1, Some(since));
        let config = Config {
            gap_timeout: Duration::from_millis(501),
        };
        assert!(
            plan(vec![3], state, now, |row| *row, config)
                .rows
                .is_empty()
        );
        let config = Config {
            gap_timeout: Duration::from_millis(500),
        };
        let plan = plan(vec![3], state, now, |row| *row, config);
        assert_eq!(plan.rows, vec![3]);
        assert_eq!(plan.state.offset, 2);
    }
}
