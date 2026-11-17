use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};
use server::app::AppError;
use uuid::Uuid;

async fn assert_response(error: AppError, status: StatusCode, code: &str) {
    assert_eq!(error.status_code(), status);
    assert_eq!(error.code(), code);
    let message = error.to_string();
    let response = error.into_response();
    assert_eq!(response.status(), status);
    assert_eq!(response.headers()["content-type"], "application/json");
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        body,
        serde_json::json!({ "error_code": code, "error": message })
    );
}

fn database_error() -> toasty::Error {
    toasty::Error::from_args(format_args!("database operation failed"))
}

#[tokio::test]
async fn application_errors_return_json_messages_and_stable_business_codes() {
    use AppError as E;
    use StatusCode as S;

    let id = Uuid::nil();
    for (error, status, code) in [
        (
            E::StorageUnavailable,
            S::SERVICE_UNAVAILABLE,
            "RAYFLOW_STORAGE_UNAVAILABLE",
        ),
        (
            E::ArtifactNotFound("/output.csv".into()),
            S::NOT_FOUND,
            "RAYFLOW_ARTIFACT_NOT_FOUND",
        ),
        (
            E::StorageConfiguration(server::storage::Error::InvalidBucket),
            S::BAD_REQUEST,
            "RAYFLOW_INVALID_STORAGE_CONFIGURATION",
        ),
        (
            E::ArtifactPath(catalog::artifact::PathError),
            S::BAD_REQUEST,
            "RAYFLOW_INVALID_ARTIFACT_PATH",
        ),
        (
            E::InvalidRequest("invalid cursor".into()),
            S::BAD_REQUEST,
            "RAYFLOW_INVALID_REQUEST",
        ),
        (E::RunIdNotFound(id), S::NOT_FOUND, "RAYFLOW_RUN_NOT_FOUND"),
        (
            E::RunNotFound {
                data_id: id,
                graph_id: id,
            },
            S::NOT_FOUND,
            "RAYFLOW_RUN_NOT_FOUND",
        ),
        (
            E::NodeRunNotFound(id),
            S::NOT_FOUND,
            "RAYFLOW_NODE_RUN_NOT_FOUND",
        ),
        (E::TaskNotFound(id), S::NOT_FOUND, "RAYFLOW_TASK_NOT_FOUND"),
        (E::JobNotFound(id), S::NOT_FOUND, "RAYFLOW_JOB_NOT_FOUND"),
        (E::JobConflict(id), S::CONFLICT, "RAYFLOW_JOB_CONFLICT"),
        (E::NodeNotFound(id), S::NOT_FOUND, "RAYFLOW_NODE_NOT_FOUND"),
        (
            E::GraphIdMismatch {
                expected: id,
                actual: Uuid::from_u128(1),
            },
            S::BAD_REQUEST,
            "RAYFLOW_GRAPH_ID_MISMATCH",
        ),
        (
            E::GraphDraftNotFound(id),
            S::NOT_FOUND,
            "RAYFLOW_GRAPH_DRAFT_NOT_FOUND",
        ),
        (
            E::InvalidGraph(graph::validator::WorkflowValidationError::EmptyGraphName),
            S::BAD_REQUEST,
            "RAYFLOW_INVALID_GRAPH",
        ),
        (
            E::Graph(Box::new(std::io::Error::other("custom store failed"))),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_GRAPH_ERROR",
        ),
        (
            E::Backend(database_error().into()),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_DATABASE_ERROR",
        ),
        (
            E::Scheduled(scheduler::Error::JobLeaseNotFound(7)),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_JOB_LEASE_NOT_FOUND",
        ),
    ] {
        assert_response(error, status, code).await;
    }
}

#[tokio::test]
async fn catalog_errors_keep_their_codes_through_storage_wrappers() {
    use StatusCode as S;
    use catalog::Error as E;

    for wrapped in [false, true] {
        for (error, status, code) in [
            (E::InvalidUrl, S::BAD_REQUEST, "RAYFLOW_INVALID_CATALOG_URL"),
            (
                E::InvalidField("key"),
                S::BAD_REQUEST,
                "RAYFLOW_INVALID_CATALOG_FIELD",
            ),
            (
                E::MissingField("key"),
                S::BAD_REQUEST,
                "RAYFLOW_MISSING_CATALOG_FIELD",
            ),
            (
                E::CatalogNotFound(Uuid::nil()),
                S::NOT_FOUND,
                "RAYFLOW_CATALOG_NOT_FOUND",
            ),
            (
                E::FileNotFound(Uuid::nil()),
                S::NOT_FOUND,
                "RAYFLOW_FILE_NOT_FOUND",
            ),
            (
                E::Database(database_error()),
                S::INTERNAL_SERVER_ERROR,
                "RAYFLOW_DATABASE_ERROR",
            ),
        ] {
            let error = if wrapped {
                AppError::Storage(Box::new(catalog::s3::Error::FileSystem(error)))
            } else {
                AppError::Catalog(error)
            };
            assert_response(error, status, code).await;
        }
    }
}

#[tokio::test]
async fn runtime_errors_keep_domain_statuses_and_codes_through_scheduler_wrappers() {
    use StatusCode as S;
    use runtime::Error as E;

    let id = Uuid::nil();
    for wrapped in [false, true] {
        for (error, status, code) in [
            (
                E::AlreadyStarted,
                S::CONFLICT,
                "RAYFLOW_RUN_ALREADY_STARTED",
            ),
            (
                E::InvalidUpgradeVersion {
                    current: 2,
                    requested: 1,
                },
                S::BAD_REQUEST,
                "RAYFLOW_INVALID_UPGRADE_VERSION",
            ),
            (
                E::InvalidUpgradeNode(id),
                S::BAD_REQUEST,
                "RAYFLOW_INVALID_UPGRADE_NODE",
            ),
            (E::NodeNotFound(id), S::NOT_FOUND, "RAYFLOW_NODE_NOT_FOUND"),
            (
                E::NodeRunNotFound(id),
                S::NOT_FOUND,
                "RAYFLOW_NODE_RUN_NOT_FOUND",
            ),
            (
                E::RunNotFound {
                    data_id: id,
                    graph_id: id,
                },
                S::NOT_FOUND,
                "RAYFLOW_RUN_NOT_FOUND",
            ),
            (
                E::InvalidTransition {
                    node: id,
                    expected: "Ready".into(),
                    status: "Succeeded".into(),
                },
                S::CONFLICT,
                "RAYFLOW_INVALID_TRANSITION",
            ),
            (
                E::ConcurrentUpdate(id),
                S::CONFLICT,
                "RAYFLOW_CONCURRENT_UPDATE",
            ),
            (
                E::Toasty(database_error()),
                S::INTERNAL_SERVER_ERROR,
                "RAYFLOW_DATABASE_ERROR",
            ),
            (
                E::Graph(Box::new(graph::store::Error::GraphNotFound {
                    graph_id: id,
                    version: Some(3),
                })),
                S::NOT_FOUND,
                "RAYFLOW_GRAPH_NOT_FOUND",
            ),
            (
                E::Event(event::Error::TaskNotFound(id)),
                S::NOT_FOUND,
                "RAYFLOW_TASK_NOT_FOUND",
            ),
        ] {
            let error = if wrapped {
                AppError::from(scheduler::Error::Runtime(error))
            } else {
                AppError::from(error)
            };
            assert_response(error, status, code).await;
        }
    }
}

#[tokio::test]
async fn event_errors_have_consistent_responses_across_adapters() {
    use StatusCode as S;
    use event::Error as E;

    for scheduled in [false, true] {
        for (error, status, code) in [
            (
                E::Toasty(database_error()),
                S::INTERNAL_SERVER_ERROR,
                "RAYFLOW_DATABASE_ERROR",
            ),
            (
                E::CommandNotFound(Uuid::nil()),
                S::NOT_FOUND,
                "RAYFLOW_COMMAND_NOT_FOUND",
            ),
            (
                E::TaskNotFound(Uuid::nil()),
                S::NOT_FOUND,
                "RAYFLOW_TASK_NOT_FOUND",
            ),
            (
                E::TaskEventNotFound(Uuid::nil()),
                S::NOT_FOUND,
                "RAYFLOW_TASK_EVENT_NOT_FOUND",
            ),
            (
                E::InvalidTaskEvent("unknown".into()),
                S::BAD_REQUEST,
                "RAYFLOW_INVALID_TASK_EVENT",
            ),
        ] {
            let error = if scheduled {
                AppError::from(scheduler::Error::Event(error))
            } else {
                AppError::from(error)
            };
            assert_response(error, status, code).await;
        }
    }
}

#[tokio::test]
async fn storage_and_scheduler_failures_preserve_their_http_contract() {
    use StatusCode as S;
    type E = catalog::s3::Error<catalog::Error>;

    for (error, status, code) in [
        (
            E::CatalogNotFound("missing".into()),
            S::NOT_FOUND,
            "RAYFLOW_CATALOG_NOT_FOUND",
        ),
        (
            E::InvalidObjectUrl,
            S::BAD_REQUEST,
            "RAYFLOW_INVALID_OBJECT_URL",
        ),
        (
            E::InvalidExpiry,
            S::BAD_REQUEST,
            "RAYFLOW_INVALID_PRESIGN_EXPIRY",
        ),
        (
            E::InvalidMultipart,
            S::BAD_REQUEST,
            "RAYFLOW_INVALID_MULTIPART_OPERATION",
        ),
        (
            E::ExpiredCredentials,
            S::SERVICE_UNAVAILABLE,
            "RAYFLOW_STORAGE_CREDENTIALS_EXPIRED",
        ),
        (
            E::Timestamp("invalid".parse::<jiff::Timestamp>().unwrap_err()),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_STORAGE_ERROR",
        ),
    ] {
        assert_response(AppError::Storage(Box::new(error)), status, code).await;
    }
    assert_response(
        AppError::Storage(Box::new(std::io::Error::other("custom provider failed"))),
        S::INTERNAL_SERVER_ERROR,
        "RAYFLOW_STORAGE_ERROR",
    )
    .await;
    use scheduler::Error as SchedulerError;
    for (error, status, code) in [
        (
            SchedulerError::InvalidTimeoutGap,
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_INVALID_JOB_TIMEOUT_GAP",
        ),
        (
            SchedulerError::Toasty(database_error()),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_DATABASE_ERROR",
        ),
        (
            SchedulerError::NodeRunNotFound(1),
            S::NOT_FOUND,
            "RAYFLOW_NODE_RUN_NOT_FOUND",
        ),
        (
            SchedulerError::RunNotFound(Uuid::nil()),
            S::NOT_FOUND,
            "RAYFLOW_RUN_NOT_FOUND",
        ),
        (
            SchedulerError::JobNotFound(Uuid::nil()),
            S::NOT_FOUND,
            "RAYFLOW_JOB_NOT_FOUND",
        ),
        (
            SchedulerError::AgentJobNotFound(Uuid::nil()),
            S::NOT_FOUND,
            "RAYFLOW_JOB_NOT_FOUND",
        ),
        (
            SchedulerError::JobConflict(Uuid::nil()),
            S::CONFLICT,
            "RAYFLOW_JOB_CONFLICT",
        ),
        (
            SchedulerError::InvalidJobLease(1),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_INVALID_JOB_LEASE",
        ),
        (
            SchedulerError::JobLeaseNotFound(1),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_JOB_LEASE_NOT_FOUND",
        ),
    ] {
        assert_response(AppError::Scheduled(error), status, code).await;
    }
}

#[tokio::test]
async fn missing_graphs_and_invalid_drafts_have_consistent_responses_across_stores() {
    use StatusCode as S;
    use graph::store::Error as E;

    for (error, status, code) in [
        (
            E::GraphNotFound {
                graph_id: Uuid::nil(),
                version: None,
            },
            S::NOT_FOUND,
            "RAYFLOW_GRAPH_NOT_FOUND",
        ),
        (
            E::DraftNotFound(Uuid::nil()),
            S::NOT_FOUND,
            "RAYFLOW_GRAPH_DRAFT_NOT_FOUND",
        ),
        (
            E::InvalidDraft(graph::validator::WorkflowValidationError::EmptyGraphName),
            S::BAD_REQUEST,
            "RAYFLOW_INVALID_GRAPH",
        ),
        (
            E::Toasty(database_error()),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_DATABASE_ERROR",
        ),
        (
            E::MetadataMismatch {
                graph_id: Uuid::nil(),
                version: 1,
            },
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_GRAPH_METADATA_MISMATCH",
        ),
        (
            E::DraftMetadataMismatch(Uuid::nil()),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_GRAPH_DRAFT_METADATA_MISMATCH",
        ),
        (
            E::VersionOverflow(Uuid::nil()),
            S::INTERNAL_SERVER_ERROR,
            "RAYFLOW_GRAPH_VERSION_OVERFLOW",
        ),
    ] {
        assert_response(AppError::Graph(Box::new(error)), status, code).await;
    }
    assert_response(
        AppError::Graph(Box::new(server::graph::GraphNotFound(Uuid::nil()))),
        S::NOT_FOUND,
        "RAYFLOW_GRAPH_NOT_FOUND",
    )
    .await;
}
