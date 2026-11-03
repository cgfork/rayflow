//! Cursor pagination shared by graph stores and their callers.

use serde::{Deserialize, Serialize};

/// A validated request for rows preceding an exclusive cursor.
#[derive(Clone, Copy, Debug)]
pub struct PageRequest {
    before: Option<u64>,
    limit: usize,
}

/// Page sizes must be bounded and nonzero.
#[derive(Debug, thiserror::Error)]
#[error("limit must be 1 to 200")]
pub struct InvalidPage;

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            before: None,
            limit: 50,
        }
    }
}

impl PageRequest {
    pub fn new(before: Option<u64>, limit: usize) -> Result<Self, InvalidPage> {
        if !(1..=200).contains(&limit) {
            return Err(InvalidPage);
        }
        Ok(Self { before, limit })
    }

    pub fn before(self) -> Option<u64> {
        self.before
    }

    pub fn limit(self) -> usize {
        self.limit
    }

    /// Pages entries already ordered by descending cursor.
    pub fn select<T>(self, entries: impl IntoIterator<Item = (u64, T)>) -> Page<T> {
        let entries = entries
            .into_iter()
            .filter(|(cursor, _)| self.before.is_none_or(|before| *cursor < before))
            .take(self.limit + 1)
            .collect();
        Page::new(entries, self.limit, |(cursor, _)| *cursor).map(|(_, item)| item)
    }
}

/// A page and the exclusive cursor for the next request, if more rows exist.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<u64>,
}

impl<T> Page<T> {
    pub fn new(mut items: Vec<T>, limit: usize, id: impl Fn(&T) -> u64) -> Self {
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if has_more { items.last().map(id) } else { None };
        Self { items, next_cursor }
    }

    pub fn map<U>(self, convert: impl FnMut(T) -> U) -> Page<U> {
        Page {
            items: self.items.into_iter().map(convert).collect(),
            next_cursor: self.next_cursor,
        }
    }
}
