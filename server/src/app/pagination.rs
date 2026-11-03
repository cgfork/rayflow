pub use graph::Page;
use graph::PageRequest;
use serde::Deserialize;

use super::AppError;

#[derive(Debug, Default, Deserialize)]
pub struct PageQuery {
    pub before: Option<u64>,
    pub limit: Option<usize>,
}

impl PageQuery {
    pub(crate) fn limit(&self) -> Result<usize, AppError> {
        Ok(self.request()?.limit())
    }

    pub(crate) fn request(&self) -> Result<PageRequest, AppError> {
        PageRequest::new(self.before, self.limit.unwrap_or(50))
            .map_err(|error| AppError::InvalidRequest(error.to_string()))
    }
}
