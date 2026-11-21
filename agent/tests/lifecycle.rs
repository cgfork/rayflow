use std::{
    collections::HashMap,
    io,
    panic::{AssertUnwindSafe, resume_unwind},
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent::{
    Agentd, JobOutcome, StopReason,
    containerd::{
        Container, ContainerEvent, ContainerState, Containerd, ExitStatus,
        docker::{DockerContainerd, DockerFiles, DockerSpec},
    },
    errors::Error,
    fs::{GraphCache, JobFs, JobResult},
    settings::Settings,
};
use event::task::Task;
use futures_util::{FutureExt, Stream, stream};
use graph::types::{DAG, Kind, Node, NodeSettings, Output};
use jiff::Timestamp;
use runtime::command::{NodeRun, Run};
use scheduler::{
    Job, ResourceCapacity,
    actuator::{NodeConfig, Status},
};
use serde_json::json;
use tempfile::{TempDir, tempdir};
use tokio::{fs, time::timeout};
use uuid::Uuid;

struct Workflow {
    _root: TempDir,
    settings: Settings,
    graph: GraphCache,
    run: Run,
    node_run: NodeRun,
}

impl Workflow {
    async fn new(output: bool) -> Self {
        let root = tempdir().unwrap();
        let settings = Settings::new(
            Uuid::new_v4(),
            "http://127.0.0.1:8080",
            root.path().join("jobs"),
            ResourceCapacity {
                cpu_cores: 2,
                memory_bytes: 128 * 1024 * 1024,
            },
        );
        let graph = GraphCache::new(settings.clone());
        let now = Timestamp::now();
        let run = Run {
            id: 1,
            uuid: Uuid::new_v4(),
            data_id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            version: 4,
            revision: 2,
            created_at: now,
            updated_at: now,
        };
        let node_run = NodeRun {
            id: 1,
            uuid: Uuid::new_v4(),
            run_id: run.uuid,
            data_id: run.data_id,
            graph_id: run.graph_id,
            version: run.version,
            node_id: Uuid::new_v4(),
            revision: run.revision,
            status: "running".into(),
            ineffective: 0,
            created_at: now,
            updated_at: now,
            run: run.clone().into(),
        };
        graph
            .write_graph(DAG {
                id: run.graph_id,
                name: "agent-lifecycle".into(),
                version: run.version,
                nodes: vec![Node {
                    id: node_run.node_id,
                    name: "execute".into(),
                    node_type: "docker".into(),
                    settings: NodeSettings::default(),
                    inputs: vec![],
                    outputs: output
                        .then(|| Output {
                            file_name: "result.txt".into(),
                            kind: Kind::File,
                            multiple: false,
                        })
                        .into_iter()
                        .collect(),
                }],
                ..DAG::default()
            })
            .await
            .unwrap();
        // The job must use its pinned version even after the node is removed.
        graph
            .write_graph(DAG {
                id: run.graph_id,
                name: "agent-lifecycle".into(),
                version: run.version + 1,
                ..DAG::default()
            })
            .await
            .unwrap();
        Self {
            _root: root,
            settings,
            graph,
            run,
            node_run,
        }
    }

    fn claim(&self, image: &str, command: &str) -> Job {
        let now = Timestamp::now();
        let config = NodeConfig {
            id: 1,
            uuid: Uuid::new_v4(),
            node_id: self.node_run.node_id,
            version: self.run.version,
            annotation_match: String::new(),
            config: json!({"image": image, "command": ["/bin/sh", "-c", command]}),
            lease: Some(30_000),
            required_cpu_cores: 1,
            required_memory_bytes: 64 * 1024 * 1024,
            created_at: now,
            updated_at: now,
            created_by: "test".into(),
            updated_by: "test".into(),
        };
        let task = Task::new(
            self.run.data_id,
            self.run.graph_id,
            self.node_run.node_id,
            self.node_run.id,
        );
        let mut job = Job::new(&task, self.run.uuid, config.uuid, Uuid::new_v4());
        job.node_run_id = self.node_run.uuid;
        job.status = Status::Polled;
        job.agent_id = Some(self.settings.id);
        job.polled_at = Some(now);
        job.expired_at = Some(now.checked_add(config.job_lease().unwrap()).unwrap());
        job.required_cpu_cores = config.required_cpu_cores;
        job.required_memory_bytes = config.required_memory_bytes;
        job.run = self.run.clone().into();
        job.node_run = self.node_run.clone().into();
        job.node_config = Some(config).into();
        job
    }

    async fn open<C: Containerd>(&self, provider: C) -> Agentd<C, GraphCache> {
        Agentd::open(provider, self.graph.clone(), self.settings.clone())
            .await
            .unwrap()
    }
}

#[derive(Clone, Default)]
struct FakeProvider(Arc<Mutex<FakeState>>);

#[derive(Default)]
struct FakeState {
    prepared: Vec<DockerSpec>,
    containers: HashMap<String, ContainerState>,
    starts: usize,
    stops: usize,
    kills: usize,
}

impl FakeProvider {
    fn exit(&self, id: &str, exit_code: i32) -> Timestamp {
        let finished_at = Timestamp::now();
        *self.0.lock().unwrap().containers.get_mut(id).unwrap() = ContainerState::Exited {
            exit_code,
            finished_at: Some(finished_at),
        };
        finished_at
    }
}

#[derive(Clone)]
struct FakeContainer {
    provider: FakeProvider,
    id: String,
}

impl Container for FakeContainer {
    fn id(&self) -> &str {
        &self.id
    }

    async fn start(&self) -> Result<(), Error> {
        let mut provider = self.provider.0.lock().unwrap();
        provider.starts += 1;
        *provider.containers.get_mut(&self.id).unwrap() = ContainerState::Running {
            pid: Some(42),
            started_at: Some(Timestamp::now()),
        };
        Ok(())
    }

    async fn wait(&self) -> Result<ExitStatus, Error> {
        loop {
            if let ContainerState::Exited {
                exit_code,
                finished_at,
            } = self.inspect().await?
            {
                return Ok(ExitStatus {
                    exit_code,
                    finished_at,
                });
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn inspect(&self) -> Result<ContainerState, Error> {
        self.provider
            .0
            .lock()
            .unwrap()
            .containers
            .get(&self.id)
            .cloned()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound).into())
    }

    async fn stop(&self) -> Result<(), Error> {
        self.provider.0.lock().unwrap().stops += 1;
        self.provider.exit(&self.id, 143);
        Ok(())
    }

    async fn kill(&self) -> Result<(), Error> {
        self.provider.0.lock().unwrap().kills += 1;
        self.provider.exit(&self.id, 137);
        Ok(())
    }

    async fn remove(&self) -> Result<(), Error> {
        self.provider.0.lock().unwrap().containers.remove(&self.id);
        Ok(())
    }
}

impl Containerd for FakeProvider {
    type Container = FakeContainer;
    type Spec<'a> = DockerSpec;

    fn name(&self) -> &'static str {
        "test"
    }

    fn spec<'a>(&'a self, job: &'a Job) -> Result<Self::Spec<'a>, Error> {
        DockerSpec::try_from(job)
    }

    fn spec_with_files<'a>(
        &'a self,
        job: &'a Job,
        metadata: &Path,
        inputs: &Path,
        outputs: &Path,
    ) -> Result<Self::Spec<'a>, Error> {
        let mut spec = self.spec(job)?;
        spec.files = Some(DockerFiles {
            metadata: metadata.into(),
            inputs: inputs.into(),
            outputs: outputs.into(),
        });
        Ok(spec)
    }

    async fn prepare(&self, spec: Self::Spec<'_>) -> Result<(), Error> {
        self.0.lock().unwrap().prepared.push(spec);
        Ok(())
    }

    async fn create(&self, spec: Self::Spec<'_>) -> Result<Self::Container, Error> {
        self.0
            .lock()
            .unwrap()
            .containers
            .entry(spec.name.clone())
            .or_insert(ContainerState::Created);
        Ok(self.container(&spec.name))
    }

    fn container(&self, id: &str) -> Self::Container {
        FakeContainer {
            provider: self.clone(),
            id: id.into(),
        }
    }

    fn events(&self) -> Pin<Box<dyn Stream<Item = Result<ContainerEvent, Error>> + Send>> {
        Box::pin(stream::empty())
    }
}

#[tokio::test]
async fn yield_and_resume_isolate_job_data_and_only_acknowledge_finished_jobs() {
    let workflow = Workflow::new(true).await;
    let provider = FakeProvider::default();
    let daemon = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "true");
    let mut files = daemon.job_fs(&job);

    assert_eq!(
        files.input_dir(),
        files.data_dir().join(job.uuid.to_string()).join("inputs")
    );
    assert_eq!(
        files.output_dir(),
        files.data_dir().join(job.uuid.to_string()).join("outputs")
    );
    daemon.prepare(job.clone()).await.unwrap();
    let spec = provider.0.lock().unwrap().prepared[0].clone();
    assert_eq!(spec.image, "fixture-image");
    let mounts = spec.files.unwrap();
    assert_eq!(mounts.inputs, files.input_dir().canonicalize().unwrap());
    assert_eq!(mounts.outputs, files.output_dir().canonicalize().unwrap());
    assert_eq!(mounts.metadata, files.dir().canonicalize().unwrap());
    assert!(files.dir().join("graph.json").exists());
    fs::write(files.output_dir().join("result.txt"), b"checkpoint")
        .await
        .unwrap();

    daemon.start(job.uuid).await.unwrap();
    assert!(daemon.acknowledge_result(job.uuid).await.is_err());
    assert!(
        provider
            .container(&spec.name)
            .inspect()
            .await
            .unwrap()
            .is_running()
    );
    daemon.stop(job.uuid, StopReason::Yielded).await.unwrap();

    let result = daemon.result(job.uuid).await.unwrap().unwrap();
    assert_eq!(result.outcome, JobOutcome::Yielded);
    assert_eq!(result.exit_code, 0);
    assert!(result.duration.is_some());
    drop(daemon);

    let daemon = workflow.open(provider.clone()).await;
    assert_eq!(daemon.poll_results().await.unwrap(), vec![result]);
    assert!(files.data_dir().join(job.uuid.to_string()).exists());
    daemon.acknowledge_result(job.uuid).await.unwrap();
    assert!(daemon.poll_results().await.unwrap().is_empty());
    assert!(daemon.jobs().await.unwrap().is_empty());
    assert!(!files.dir().join("job.json").exists());
    assert!(!files.dir().join("result.json").exists());
    assert!(!files.data_dir().join(job.uuid.to_string()).exists());
    files.clear_data().await.unwrap();
    files.clear_data().await.unwrap();
    assert!(provider.container(&spec.name).inspect().await.is_err());
    assert_eq!(provider.0.lock().unwrap().stops, 1);

    let resumed = workflow.claim("fixture-image", "true");
    let resumed_files = daemon.job_fs(&resumed);
    assert_ne!(resumed.uuid, job.uuid);
    assert_eq!(resumed.node_run_id, job.node_run_id);
    assert_ne!(resumed_files.output_dir(), files.output_dir());
    assert_ne!(
        resumed_files.state_dir(resumed.uuid),
        files.state_dir(job.uuid)
    );
    assert!(!resumed_files.output_dir().join("result.txt").exists());
    daemon.prepare(resumed.clone()).await.unwrap();
    daemon.start(resumed.uuid).await.unwrap();
    let finished_at = provider.exit(&format!("rayflow-{}", resumed.uuid), 0);
    drop(daemon);

    let daemon = workflow.open(provider).await;
    let result = daemon.result(resumed.uuid).await.unwrap().unwrap();
    assert_eq!(result.outcome, JobOutcome::Succeeded);
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.finished_at, Some(finished_at));
    assert!(daemon.jobs().await.unwrap().is_empty());
}

#[tokio::test]
async fn restarting_during_execution_preserves_container_and_recovers_failure() {
    let workflow = Workflow::new(false).await;
    let provider = FakeProvider::default();
    let daemon = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "exit 7");
    daemon.prepare(job.clone()).await.unwrap();
    let files = daemon.job_fs(&job);
    fs::create_dir_all(files.output_dir()).await.unwrap();
    fs::write(files.output_dir().join("checkpoint.txt"), b"pending report")
        .await
        .unwrap();
    daemon.start(job.uuid).await.unwrap();
    drop(daemon);

    let daemon = workflow.open(provider.clone()).await;
    let jobs = daemon.jobs().await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].uuid, job.uuid);
    let mut redelivery = job.clone();
    redelivery.expired_at = Some(Timestamp::now());
    assert!(matches!(
        daemon.prepare(redelivery).await,
        Err(Error::JobRunning(id)) if id == job.uuid
    ));
    assert_eq!(daemon.jobs().await.unwrap()[0].expired_at, job.expired_at);
    daemon.start(job.uuid).await.unwrap();
    assert_eq!(provider.0.lock().unwrap().starts, 1);

    let finished_at = provider.exit(&format!("rayflow-{}", job.uuid), 7);
    drop(daemon);
    let daemon = workflow.open(provider).await;
    let result = daemon.result(job.uuid).await.unwrap().unwrap();
    assert_eq!(result.outcome, JobOutcome::Failed);
    assert_eq!(result.exit_code, 7);
    assert_eq!(result.finished_at, Some(finished_at));
    assert!(result.started_at.is_some());
    assert!(result.duration.is_some());
    assert!(
        daemon
            .job_fs(&job)
            .data_dir()
            .join(job.uuid.to_string())
            .exists()
    );
}

#[tokio::test]
async fn start_waits_for_the_current_result_under_the_job_lock() {
    let workflow = Workflow::new(false).await;
    let provider = FakeProvider::default();
    let daemon = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "sleep 30");
    daemon.prepare(job.clone()).await.unwrap();
    daemon.start(job.uuid).await.unwrap();

    let mut files = daemon.job_fs(&job);
    files.lock().await.unwrap();
    let mut start = Box::pin(daemon.start(job.uuid));
    assert!(
        timeout(Duration::from_millis(50), &mut start)
            .await
            .is_err()
    );
    let finished_at = provider.exit(&format!("rayflow-{}", job.uuid), 143);
    files
        .write_job_result(JobResult {
            exit_code: 0,
            outcome: Some("yield".into()),
            finished_at: Some(finished_at),
            duration_ms: None,
        })
        .await
        .unwrap();
    files.unlock().await.unwrap();
    start.await.unwrap();

    let result = daemon.result(job.uuid).await.unwrap().unwrap();
    assert_eq!(result.outcome, JobOutcome::Yielded);
    assert_eq!(result.exit_code, 0);
    assert_eq!(provider.0.lock().unwrap().starts, 1);
}

#[tokio::test]
async fn expired_running_job_is_killed_and_reported_when_events_are_unavailable() {
    let workflow = Workflow::new(false).await;
    let provider = FakeProvider::default();
    let daemon = workflow.open(provider.clone()).await;
    let mut job = workflow.claim("fixture-image", "sleep 30");
    job.expired_at = Some(
        Timestamp::now()
            .checked_add(Duration::from_secs(1))
            .unwrap(),
    );
    daemon.prepare(job.clone()).await.unwrap();
    daemon.start(job.uuid).await.unwrap();
    assert!(
        provider
            .container(&format!("rayflow-{}", job.uuid))
            .inspect()
            .await
            .unwrap()
            .is_running()
    );

    let mut watch = Box::pin(daemon.watch());
    let results = timeout(Duration::from_secs(4), async {
        tokio::select! {
            result = &mut watch => panic!("watcher stopped early: {result:?}"),
            results = daemon.wait_results() => results.unwrap(),
        }
    })
    .await
    .expect("periodic reconciliation must work despite reconnecting events");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].job_id, job.uuid);
    assert_eq!(results[0].outcome, JobOutcome::Failed);
    assert_eq!(results[0].exit_code, 124);
    assert_eq!(provider.0.lock().unwrap().kills, 1);

    daemon.shutdown();
    timeout(Duration::from_secs(1), watch)
        .await
        .unwrap()
        .unwrap();
    daemon.acknowledge_result(job.uuid).await.unwrap();
    assert!(daemon.poll_results().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires a Docker daemon and a local shell image in RAYFLOW_TEST_DOCKER_IMAGE or busybox:latest"]
async fn docker_exit_statuses_are_recovered_and_acknowledged() {
    let image =
        std::env::var("RAYFLOW_TEST_DOCKER_IMAGE").unwrap_or_else(|_| "busybox:latest".into());
    let workflow = Workflow::new(false).await;
    let docker = DockerContainerd::connect_with_defaults().unwrap();
    docker.docker().inspect_image(&image).await.unwrap();
    let jobs = [
        (workflow.claim(&image, "exit 0"), JobOutcome::Succeeded, 0),
        (workflow.claim(&image, "exit 7"), JobOutcome::Failed, 7),
    ];
    let docker_for_test = docker.clone();
    let test_jobs = jobs.clone();
    let result = AssertUnwindSafe(async move {
        let mut daemon = workflow.open(docker_for_test.clone()).await;
        for (job, outcome, exit_code) in test_jobs {
            daemon.prepare(job.clone()).await.unwrap();
            daemon.start(job.uuid).await.unwrap();
            let container_name = format!("rayflow-{}", job.uuid);
            let container = docker_for_test.container(&container_name);
            let status = timeout(Duration::from_secs(10), container.wait())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(status.exit_code, exit_code);

            drop(daemon);
            daemon = workflow.open(docker_for_test.clone()).await;
            let result = daemon.result(job.uuid).await.unwrap().unwrap();
            assert_eq!(result.outcome, outcome);
            assert_eq!(result.exit_code, exit_code);
            daemon.acknowledge_result(job.uuid).await.unwrap();
            assert!(container.inspect().await.is_err());
            assert!(daemon.poll_results().await.unwrap().is_empty());
        }
    })
    .catch_unwind()
    .await;

    for (job, _, _) in jobs {
        docker
            .container(&format!("rayflow-{}", job.uuid))
            .remove()
            .await
            .unwrap();
    }
    if let Err(panic) = result {
        resume_unwind(panic);
    }
}
