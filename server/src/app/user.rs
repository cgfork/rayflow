use std::collections::HashMap;

use crate::auth::UserRecord;

use super::{App, AppError};

const MAX_USER_IDS: usize = 100;

impl<S> App<S> {
    pub(crate) async fn resolve_users(
        &self,
        mut ids: Vec<String>,
    ) -> Result<HashMap<String, String>, AppError> {
        if ids.len() > MAX_USER_IDS {
            return Err(AppError::InvalidRequest(format!(
                "at most {MAX_USER_IDS} user IDs can be resolved at once"
            )));
        }
        if ids
            .iter()
            .any(|id| id.len() != 64 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(AppError::InvalidRequest(
                "user IDs must be 64 hexadecimal characters".into(),
            ));
        }
        if !self.config.auth.enabled || ids.is_empty() {
            return Ok(HashMap::new());
        }

        ids.sort_unstable();
        ids.dedup();
        let rows = UserRecord::filter(UserRecord::fields().user_id().in_list(ids))
            .exec(&mut self.db.clone())
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let name = row.profile.get("name")?.as_str()?;
                (!name.trim().is_empty()).then(|| (row.user_id, name.to_owned()))
            })
            .collect())
    }
}
