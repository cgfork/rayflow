//! Container runtime access.
//!
//! This module deliberately knows nothing about Rayflow jobs or the local
//! filesystem. It is the provider boundary used by `agentd`.

use std::pin::Pin;

use futures_util::Stream;
use jiff::Timestamp;
use scheduler::Job;

use crate::errors::Error;

pub mod docker;

/// The identifier used by a container provider to address a container.
///
/// Providers may use a UUID, a generated name, or another opaque identifier.
/// Callers must not parse or manufacture this value; they should persist it
/// and pass it back to [`Containerd::container`] when recovering a job.
pub type ContainerId = String;

/// The provider-observed lifecycle state of a container.
///
/// This is intentionally smaller than any provider's native state machine.
/// `agentd` only needs the states required to recover jobs, decide whether a
/// container can be started, and produce a durable job result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContainerState {
    /// The container exists but has not started executing.
    Created,
    /// The container is currently executing or is temporarily paused by the
    /// provider.
    Running {
        /// The operating-system process ID, when the provider exposes one.
        pid: Option<i64>,
        /// The provider timestamp at which execution began, when available.
        started_at: Option<Timestamp>,
    },
    /// The container reached a terminal provider state.
    Exited {
        /// The process exit code. A non-zero value represents job failure
        /// unless the agent deliberately maps the result to another outcome.
        exit_code: i32,
        /// The provider timestamp at which execution ended, when available.
        finished_at: Option<Timestamp>,
    },
}

impl ContainerState {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    pub fn exit_code(&self) -> Option<i32> {
        match self {
            Self::Exited { exit_code, .. } => Some(*exit_code),
            _ => None,
        }
    }
}

/// The terminal status returned after waiting for a container.
///
/// `wait` is the authoritative completion operation for a running container.
/// The timestamp may be absent when a provider does not expose one; callers
/// should then use their own observation time rather than treating the value
/// as a guaranteed provider timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExitStatus {
    /// The process exit code reported by the provider.
    pub exit_code: i32,
    /// The provider-reported completion time, if available.
    pub finished_at: Option<Timestamp>,
}

/// A normalized provider event used to wake the agent's reconciliation loop.
///
/// Events are hints, not durable state. A dropped event stream, an agent crash,
/// or a provider restart must not lose a job result. The agent therefore uses
/// the event to trigger `inspect`/`wait` and relies on the persisted job files
/// plus provider inspection for recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContainerAction {
    /// A provider created a container.
    Create,
    /// A provider started a container.
    Start,
    /// A container process exited.
    Die,
    /// A provider stopped a container gracefully.
    Stop,
    /// A provider forcefully terminated a container.
    Kill,
    /// A provider removed a container.
    Destroy,
    /// A provider event that has no dedicated normalized action yet.
    Other(String),
}

/// A provider event associated with a container identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerEvent {
    /// The container affected by the event.
    pub container_id: ContainerId,
    /// The normalized provider action.
    pub action: ContainerAction,
    /// The time at which the agent observed the event.
    pub occurred_at: Timestamp,
}

/// A provider-neutral handle for one isolated job container.
///
/// The handle is cheap to clone and contains only the provider identity. The
/// containerd implementation owns the provider connection and translates
/// these lifecycle operations into provider-specific API calls.
#[allow(async_fn_in_trait)]
pub trait Container: Clone + Send + Sync + 'static {
    /// Returns the provider-specific container identifier.
    fn id(&self) -> &str;

    /// Starts the container, treating an already-running container as success.
    async fn start(&self) -> Result<(), Error>;

    /// Waits until the container exits and returns its exit status.
    async fn wait(&self) -> Result<ExitStatus, Error>;

    /// Reads the current provider state for crash recovery and reconciliation.
    async fn inspect(&self) -> Result<ContainerState, Error>;

    /// Requests a graceful container shutdown.
    async fn stop(&self) -> Result<(), Error>;

    /// Forcefully terminates the container.
    async fn kill(&self) -> Result<(), Error>;

    /// Removes the container and its provider-side metadata.
    async fn remove(&self) -> Result<(), Error>;
}

/// The provider connection and container factory used by `agentd`.
///
/// Implementations own all provider-specific details: image preparation,
/// resource limits, container creation, lifecycle calls, and event decoding.
/// The rest of the agent interacts only with this trait, so a Docker-backed
/// implementation can be replaced by another runtime without changing job
/// persistence or remote polling logic.
#[allow(async_fn_in_trait)]
pub trait Containerd: Clone + Send + Sync + 'static {
    /// Stable provider label persisted in `container.json`.
    fn name(&self) -> &'static str {
        "container"
    }

    /// The provider's container handle type.
    type Container: Container;

    /// The provider-specific creation input.
    ///
    /// This is a generic associated type because each provider may need a
    /// different specification. For example, Docker needs an image, name,
    /// CPU limit, and memory limit, while another provider may require a pod
    /// manifest or a sandbox descriptor. The specification may borrow the
    /// source job while it is being assembled.
    type Spec<'a>: Send + 'a
    where
        Self: 'a;

    /// Builds a provider-specific specification from a pinned remote job.
    ///
    /// This method validates the information needed before any provider-side
    /// resources are created. It should not create a container or mutate the
    /// job filesystem; those actions belong to `prepare` and `create`.
    fn spec<'a>(&'a self, job: &'a Job) -> Result<Self::Spec<'a>, Error>;

    /// Makes the provider resources referenced by `spec` available.
    ///
    /// For Docker this means ensuring the image is present, usually by
    /// inspecting first and pulling only when necessary. Implementations
    /// should make this operation safe to retry after an agent restart.
    async fn prepare(&self, spec: Self::Spec<'_>) -> Result<(), Error>;

    /// Creates or reconnects to the provider container described by `spec`.
    ///
    /// The returned handle must be suitable for persisting in `JobFs` before
    /// the caller starts the container. If creation races with recovery, an
    /// implementation may return a handle to an already-existing container
    /// with the same deterministic identity.
    async fn create(&self, spec: Self::Spec<'_>) -> Result<Self::Container, Error>;

    /// Reconstructs a handle for a persisted provider container identifier.
    ///
    /// This must not contact the provider or create a new container. It is
    /// used during startup recovery before `agentd` inspects the container.
    fn container(&self, id: &str) -> Self::Container;

    /// Opens the provider's global event stream.
    ///
    /// The stream may end or yield an error and must be reconnectable. Events
    /// wake reconciliation; they do not replace the periodic recovery scan or
    /// the durable result written by `agentd`.
    fn events(&self)
    -> Pin<Box<dyn Stream<Item = Result<ContainerEvent, Error>> + Send + 'static>>;
}
