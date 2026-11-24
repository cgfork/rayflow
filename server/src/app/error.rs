//! Application errors and their HTTP status, business code, and message.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use uuid::Uuid;

/// Error returned by an app operation.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("object storage is not configured")]
    StorageUnavailable,
    #[error("artifact not found: {0}")]
    ArtifactNotFound(String),
    #[error(transparent)]
    Catalog(#[from] ::catalog::Error),
    #[error(transparent)]
    Storage(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    StorageConfiguration(#[from] crate::config::StorageConfigError),
    #[error(transparent)]
    ArtifactPath(#[from] ::catalog::path::PathError),
    #[error("{0}")]
    InvalidRequest(String),
    #[error("run not found: {0}")]
    RunIdNotFound(Uuid),
    /// The requested run does not exist.
    #[error("run not found: data {data_id}, graph {graph_id}")]
    RunNotFound {
        /// Data identifier of the run.
        data_id: Uuid,
        /// Graph identifier of the run.
        graph_id: Uuid,
    },
    /// The requested node has no run at the run's current revision.
    #[error("node run not found: {0}")]
    NodeRunNotFound(Uuid),
    /// The requested task does not exist.
    #[error("task not found: {0}")]
    TaskNotFound(Uuid),
    #[error("job not found: {0}")]
    JobNotFound(Uuid),
    #[error("job claim expired, changed owner, or no longer accepts this status: {0}")]
    JobConflict(Uuid),
    #[error("terminal job has no recorded result owner: {0}")]
    JobResultOwnerUnknown(Uuid),
    /// The requested node is not part of the graph.
    #[error("node not found in graph: {0}")]
    NodeNotFound(Uuid),
    /// The graph identifier in a draft request does not match the graph JSON.
    #[error("graph id mismatch: expected {expected}, got {actual}")]
    GraphIdMismatch { expected: Uuid, actual: Uuid },
    /// The requested graph draft does not exist.
    #[error("graph draft not found: {0}")]
    GraphDraftNotFound(Uuid),
    /// The draft cannot be published because it is not a valid graph.
    #[error("invalid graph: {0}")]
    InvalidGraph(#[source] graph::validator::WorkflowValidationError),
    /// The graph store returned an error.
    #[error("graph: {0}")]
    Graph(Box<dyn std::error::Error + Send + Sync>),
    /// The backend returned an error.
    #[error(transparent)]
    Backend(#[from] runtime::Error),
    /// Job persistence failed.
    #[error(transparent)]
    Scheduled(scheduler::Error),
}

impl From<event::Error> for AppError {
    fn from(error: event::Error) -> Self {
        match error {
            event::Error::TaskNotFound(id) => Self::TaskNotFound(id),
            error => Self::Backend(error.into()),
        }
    }
}

impl From<scheduler::Error> for AppError {
    fn from(error: scheduler::Error) -> Self {
        match error {
            scheduler::Error::AgentJobNotFound(job_id) => Self::JobNotFound(job_id),
            scheduler::Error::JobConflict(job_id) => Self::JobConflict(job_id),
            error => Self::Scheduled(error),
        }
    }
}

impl From<toasty::Error> for AppError {
    fn from(error: toasty::Error) -> Self {
        Self::Backend(error.into())
    }
}

impl From<graph::store::Error> for AppError {
    fn from(error: graph::store::Error) -> Self {
        match error {
            graph::store::Error::DraftNotFound(graph_id) => Self::GraphDraftNotFound(graph_id),
            graph::store::Error::InvalidDraft(error) => Self::InvalidGraph(error),
            error => Self::Graph(Box::new(error)),
        }
    }
}

impl AppError {
    /// HTTP status for this error, including errors wrapped by backend adapters.
    pub fn status_code(&self) -> StatusCode {
        self.http_error().0
    }

    /// Stable business error code. Clients should branch on this instead of the message.
    pub fn code(&self) -> &'static str {
        self.http_error().1
    }

    fn http_error(&self) -> (StatusCode, &'static str) {
        use StatusCode as Status;

        match self {
            Self::StorageUnavailable => {
                (Status::SERVICE_UNAVAILABLE, "RAYFLOW_STORAGE_UNAVAILABLE")
            }
            Self::ArtifactNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_ARTIFACT_NOT_FOUND"),
            Self::Catalog(error) => catalog_error(error),
            Self::Storage(error) => storage_error(error.as_ref()),
            Self::StorageConfiguration(_) => {
                (Status::BAD_REQUEST, "RAYFLOW_INVALID_STORAGE_CONFIGURATION")
            }
            Self::ArtifactPath(_) => (Status::BAD_REQUEST, "RAYFLOW_INVALID_ARTIFACT_PATH"),
            Self::InvalidRequest(_) => (Status::BAD_REQUEST, "RAYFLOW_INVALID_REQUEST"),
            Self::RunIdNotFound(_) | Self::RunNotFound { .. } => {
                (Status::NOT_FOUND, "RAYFLOW_RUN_NOT_FOUND")
            }
            Self::NodeRunNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_NODE_RUN_NOT_FOUND"),
            Self::TaskNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_TASK_NOT_FOUND"),
            Self::JobNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_JOB_NOT_FOUND"),
            Self::JobConflict(_) => (Status::CONFLICT, "RAYFLOW_JOB_CONFLICT"),
            Self::JobResultOwnerUnknown(_) => {
                (Status::CONFLICT, "RAYFLOW_JOB_RESULT_OWNER_UNKNOWN")
            }
            Self::NodeNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_NODE_NOT_FOUND"),
            Self::GraphIdMismatch { .. } => (Status::BAD_REQUEST, "RAYFLOW_GRAPH_ID_MISMATCH"),
            Self::GraphDraftNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_GRAPH_DRAFT_NOT_FOUND"),
            Self::InvalidGraph(_) => (Status::BAD_REQUEST, "RAYFLOW_INVALID_GRAPH"),
            Self::Graph(error) => graph_error(error.as_ref()),
            Self::Backend(error) => runtime_error(error),
            Self::Scheduled(error) => scheduler_error(error),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_code) = self.http_error();
        (
            status,
            Json(ErrorBody {
                error_code,
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

/// JSON body returned by an application operation that failed.
#[derive(Debug, Serialize)]
pub struct ErrorBody {
    /// Stable business code, independent of the HTTP status and message wording.
    pub error_code: &'static str,
    /// Human-readable error message.
    pub error: String,
}

fn catalog_error(error: &::catalog::Error) -> (StatusCode, &'static str) {
    use ::catalog::Error;
    use StatusCode as Status;

    match error {
        Error::InvalidUrl => (Status::BAD_REQUEST, "RAYFLOW_INVALID_CATALOG_URL"),
        Error::InvalidField(_) => (Status::BAD_REQUEST, "RAYFLOW_INVALID_CATALOG_FIELD"),
        Error::MissingField(_) => (Status::BAD_REQUEST, "RAYFLOW_MISSING_CATALOG_FIELD"),
        Error::CatalogNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_CATALOG_NOT_FOUND"),
        Error::FileNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_FILE_NOT_FOUND"),
        Error::Database(_) => (Status::INTERNAL_SERVER_ERROR, "RAYFLOW_DATABASE_ERROR"),
    }
}

fn storage_error(error: &(dyn std::error::Error + 'static)) -> (StatusCode, &'static str) {
    use ::catalog::s3::Error;
    use StatusCode as Status;

    let Some(error) = error.downcast_ref::<Error<::catalog::Error>>() else {
        return (Status::INTERNAL_SERVER_ERROR, "RAYFLOW_STORAGE_ERROR");
    };
    match error {
        Error::FileSystem(error) => catalog_error(error),
        Error::CatalogNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_CATALOG_NOT_FOUND"),
        Error::InvalidObjectUrl => (Status::BAD_REQUEST, "RAYFLOW_INVALID_OBJECT_URL"),
        Error::InvalidExpiry => (Status::BAD_REQUEST, "RAYFLOW_INVALID_PRESIGN_EXPIRY"),
        Error::InvalidMultipart => (Status::BAD_REQUEST, "RAYFLOW_INVALID_MULTIPART_OPERATION"),
        Error::ExpiredCredentials => (
            Status::SERVICE_UNAVAILABLE,
            "RAYFLOW_STORAGE_CREDENTIALS_EXPIRED",
        ),
        Error::SigningParameters(_) | Error::Signing(_) | Error::Http(_) | Error::Timestamp(_) => {
            (Status::INTERNAL_SERVER_ERROR, "RAYFLOW_STORAGE_ERROR")
        }
    }
}

fn graph_error(error: &(dyn std::error::Error + 'static)) -> (StatusCode, &'static str) {
    use StatusCode as Status;
    use graph::store::Error;

    match error.downcast_ref::<Error>() {
        Some(Error::GraphNotFound { .. }) => (Status::NOT_FOUND, "RAYFLOW_GRAPH_NOT_FOUND"),
        Some(Error::DraftNotFound(_)) => (Status::NOT_FOUND, "RAYFLOW_GRAPH_DRAFT_NOT_FOUND"),
        Some(Error::InvalidDraft(_)) => (Status::BAD_REQUEST, "RAYFLOW_INVALID_GRAPH"),
        Some(Error::Toasty(_)) => (Status::INTERNAL_SERVER_ERROR, "RAYFLOW_DATABASE_ERROR"),
        Some(Error::MetadataMismatch { .. }) => (
            Status::INTERNAL_SERVER_ERROR,
            "RAYFLOW_GRAPH_METADATA_MISMATCH",
        ),
        Some(Error::DraftMetadataMismatch(_)) => (
            Status::INTERNAL_SERVER_ERROR,
            "RAYFLOW_GRAPH_DRAFT_METADATA_MISMATCH",
        ),
        Some(Error::VersionOverflow(_)) => (
            Status::INTERNAL_SERVER_ERROR,
            "RAYFLOW_GRAPH_VERSION_OVERFLOW",
        ),
        None => (Status::INTERNAL_SERVER_ERROR, "RAYFLOW_GRAPH_ERROR"),
    }
}

fn event_error(error: &event::Error) -> (StatusCode, &'static str) {
    use StatusCode as Status;
    use event::Error;

    match error {
        Error::Toasty(_) => (Status::INTERNAL_SERVER_ERROR, "RAYFLOW_DATABASE_ERROR"),
        Error::CommandNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_COMMAND_NOT_FOUND"),
        Error::TaskNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_TASK_NOT_FOUND"),
        Error::TaskEventNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_TASK_EVENT_NOT_FOUND"),
        Error::InvalidTaskEvent(_) => (Status::BAD_REQUEST, "RAYFLOW_INVALID_TASK_EVENT"),
    }
}

fn runtime_error(error: &runtime::Error) -> (StatusCode, &'static str) {
    use StatusCode as Status;
    use runtime::Error;

    let status = match error {
        Error::Graph(error) => return graph_error(error.as_ref()),
        Error::Event(error) => return event_error(error),
        Error::NodeNotFound(_) | Error::NodeRunNotFound(_) | Error::RunNotFound { .. } => {
            Status::NOT_FOUND
        }
        Error::InvalidUpgradeVersion { .. } | Error::InvalidUpgradeNode(_) => Status::BAD_REQUEST,
        Error::AlreadyStarted | Error::InvalidTransition { .. } | Error::ConcurrentUpdate(_) => {
            Status::CONFLICT
        }
        Error::Toasty(_) => Status::INTERNAL_SERVER_ERROR,
    };
    (status, error.code())
}

fn scheduler_error(error: &scheduler::Error) -> (StatusCode, &'static str) {
    use StatusCode as Status;
    use scheduler::Error;

    match error {
        Error::Toasty(_) => (Status::INTERNAL_SERVER_ERROR, "RAYFLOW_DATABASE_ERROR"),
        Error::Event(error) => event_error(error),
        Error::Runtime(error) => runtime_error(error),
        Error::NodeRunNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_NODE_RUN_NOT_FOUND"),
        Error::RunNotFound(_) => (Status::NOT_FOUND, "RAYFLOW_RUN_NOT_FOUND"),
        Error::JobNotFound(_) | Error::AgentJobNotFound(_) => {
            (Status::NOT_FOUND, "RAYFLOW_JOB_NOT_FOUND")
        }
        Error::JobConflict(_) => (Status::CONFLICT, "RAYFLOW_JOB_CONFLICT"),
        Error::InvalidTimeoutGap => (
            Status::INTERNAL_SERVER_ERROR,
            "RAYFLOW_INVALID_JOB_TIMEOUT_GAP",
        ),
        Error::InvalidJobLease(_) => (Status::INTERNAL_SERVER_ERROR, "RAYFLOW_INVALID_JOB_LEASE"),
        Error::JobLeaseNotFound(_) => {
            (Status::INTERNAL_SERVER_ERROR, "RAYFLOW_JOB_LEASE_NOT_FOUND")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_job_claim_conflicts_to_http_conflict() {
        let error = AppError::JobConflict(Uuid::new_v4());
        assert_eq!(error.status_code(), StatusCode::CONFLICT);
        assert_eq!(error.code(), "RAYFLOW_JOB_CONFLICT");
    }

    #[test]
    fn maps_invalid_requests_to_bad_request() {
        let error = AppError::InvalidRequest("invalid node".into());
        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "RAYFLOW_INVALID_REQUEST");
    }

    #[test]
    fn maps_runtime_invalid_transitions_to_conflict() {
        let error = AppError::Backend(runtime::Error::AlreadyStarted);
        assert_eq!(error.status_code(), StatusCode::CONFLICT);
        assert_eq!(error.code(), "RAYFLOW_RUN_ALREADY_STARTED");
    }
}
