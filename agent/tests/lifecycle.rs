use std::{
    collections::HashMap,
    future::{Future, pending},
    io,
    panic::{AssertUnwindSafe, resume_unwind},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent::{
    JobManager, JobOutcome, JobResult, JobUsage, StopReason, Usage, UsagePercentiles,
    client::Client,
    containerd::{
        Container, ContainerEvent, ContainerFile, ContainerState, Containerd, ExitStatus,
        docker::{DockerContainerd, DockerSpec},
    },
    errors::Error,
    settings::Settings,
};
use event::task::Task;
use futures_util::{FutureExt, Stream, StreamExt, stream};
use graph::{
    GraphStore, Page, PageRequest,
    types::{AssetRef, DAG, Kind, Node, NodeSettings, Output},
};
use jiff::Timestamp;
use runtime::command::{NodeRun, Run};
use scheduler::{
    GpuDevice, Job, ResourceCapacity,
    actuator::{NodeConfig, Status},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::{TempDir, tempdir};
use tokio::{fs, sync::Notify, time::timeout};
use uuid::Uuid;

#[derive(Clone)]
struct Graphs([DAG; 2]);

impl GraphStore<Uuid> for Graphs {
    type G = DAG;
    type Err = io::Error;

    async fn graphs(&mut self, _: PageRequest, _: Option<Uuid>) -> Result<Page<DAG>, io::Error> {
        unreachable!("jobs load pinned graph versions")
    }

    async fn load(&mut self, id: Uuid, version: Option<u64>) -> Result<DAG, io::Error> {
        self.0
            .iter()
            .rev()
            .find(|graph| graph.id == id && version.is_none_or(|version| graph.version == version))
            .cloned()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
    }
}

struct Workflow {
    _root: TempDir,
    settings: Settings,
    graph: Graphs,
    run: Run,
    node_run: NodeRun,
}

impl Workflow {
    fn new(output: bool) -> Self {
        let root = tempdir().unwrap();
        let settings = Settings::new(
            Uuid::new_v4(),
            "http://127.0.0.1:1",
            root.path().join("jobs"),
            ResourceCapacity {
                cpu_cores: 2,
                memory_bytes: 128 * 1024 * 1024,
            },
        );
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
        let graph = Graphs([
            DAG {
                id: run.graph_id,
                name: "agent-lifecycle".into(),
                version: run.version,
                nodes: vec![Node {
                    id: node_run.node_id,
                    name: "execute".into(),
                    node_type: "docker".into(),
                    assets: vec![],
                    settings: NodeSettings::default(),
                    inputs: vec![],
                    outputs: output
                        .then(|| Output {
                            file_name: "result.txt".into(),
                            kind: Kind::File,
                        })
                        .into_iter()
                        .collect(),
                }],
                ..DAG::default()
            },
            // The job uses its pinned version even after the node is removed.
            DAG {
                id: run.graph_id,
                name: "agent-lifecycle".into(),
                version: run.version + 1,
                ..DAG::default()
            },
        ]);
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
            name: "default".into(),
            annotation_match: String::new(),
            config: json!({"image": image, "command": ["/bin/sh", "-c", command]}),
            lease: Some(30_000),
            required_cpu_cores: 1,
            required_memory_bytes: 64 * 1024 * 1024,
            required_gpu_memory_bytes: 0,
            required_gpu_cdi_device: None,
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
        job.required_gpu_memory_bytes = config.required_gpu_memory_bytes;
        job.run = self.run.clone().into();
        job.node_run = self.node_run.clone().into();
        job.node_config = Some(config).into();
        job
    }

    async fn open<C: Containerd>(&self, provider: C) -> JobManager<C, Graphs> {
        JobManager::open(
            provider,
            self.graph.clone(),
            Client::new(self.settings.clone()).unwrap(),
        )
        .await
        .unwrap()
    }

    fn job_dir(&self, job_id: Uuid) -> PathBuf {
        self.settings.workdir.join(job_id.to_string())
    }

    fn data_dir(&self, job_id: Uuid) -> PathBuf {
        self.settings.data_dir.join(job_id.to_string())
    }
}

#[derive(Clone, Default)]
struct FakeProvider(Arc<Mutex<FakeState>>);

type UsageStream = Pin<Box<dyn Stream<Item = Result<Usage, Error>> + Send>>;

#[derive(Default)]
struct FakeState {
    prepared: Vec<DockerSpec>,
    containers: HashMap<String, ContainerState>,
    creates: usize,
    lose_create_reply: bool,
    fail_inspect: bool,
    inspections: usize,
    starts: usize,
    stops: usize,
    kills: usize,
    usage: Option<Usage>,
    usage_stream: Option<UsageStream>,
    sample_started: Option<Arc<Notify>>,
    sample_fails: bool,
}

impl FakeProvider {
    fn exit(&self, id: &str, exit_code: i32) -> Timestamp {
        let finished_at = Timestamp::now();
        let mut provider = self.0.lock().unwrap();
        let state = provider.containers.get_mut(id).unwrap();
        *state = ContainerState::Exited {
            exit_code,
            started_at: state.started_at(),
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
                ..
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
        let mut provider = self.provider.0.lock().unwrap();
        provider.inspections += 1;
        if provider.fail_inspect {
            return Err(io::Error::from(io::ErrorKind::ConnectionReset).into());
        }
        provider
            .containers
            .get(&self.id)
            .cloned()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound).into())
    }

    fn usage(&self) -> UsageStream {
        if let Some(stream) = self.provider.0.lock().unwrap().usage_stream.take() {
            return stream;
        }
        let container = self.clone();
        Box::pin(stream::once(async move {
            if !container.inspect().await?.is_running() {
                return Err(io::Error::from(io::ErrorKind::NotFound).into());
            }
            let (usage, sample_started, sample_fails) = {
                let provider = container.provider.0.lock().unwrap();
                (
                    provider.usage.clone(),
                    provider.sample_started.clone(),
                    provider.sample_fails,
                )
            };
            if let Some(sample_started) = sample_started {
                sample_started.notify_one();
                if sample_fails {
                    Err(io::Error::other("stats unavailable").into())
                } else {
                    pending().await
                }
            } else {
                usage.ok_or_else(|| io::Error::from(io::ErrorKind::NotFound).into())
            }
        }))
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

    fn spec_with_files<'a, 'files>(
        &'a self,
        job: &'a Job,
        files: impl IntoIterator<Item = &'files ContainerFile>,
    ) -> Result<Self::Spec<'a>, Error> {
        let mut spec = self.spec(job)?;
        spec.files = files.into_iter().cloned().collect();
        Ok(spec)
    }

    async fn prepare(&self, spec: Self::Spec<'_>) -> Result<(), Error> {
        self.0.lock().unwrap().prepared.push(spec);
        Ok(())
    }

    async fn create(&self, spec: Self::Spec<'_>) -> Result<Self::Container, Error> {
        let mut provider = self.0.lock().unwrap();
        provider.creates += 1;
        provider
            .containers
            .entry(spec.name.clone())
            .or_insert(ContainerState::Created);
        if provider.lose_create_reply {
            return Err(io::Error::from(io::ErrorKind::ConnectionReset).into());
        }
        Ok(self.container(&spec.name))
    }

    async fn find_container(&self, job_id: Uuid) -> Result<Option<Self::Container>, Error> {
        let name = format!("rayflow-{job_id}");
        Ok(self
            .0
            .lock()
            .unwrap()
            .containers
            .contains_key(&name)
            .then(|| self.container(&name)))
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

fn example_usage() -> Usage {
    Usage {
        sampled_at: Timestamp::now(),
        cpu_usage_ns: Some(12_000_000),
        memory_usage_bytes: Some(1024),
        memory_working_set_bytes: Some(768),
        memory_limit_bytes: Some(64 * 1024 * 1024),
    }
}

async fn watch_until<T>(
    manager: &JobManager<FakeProvider, Graphs>,
    condition: impl Future<Output = T>,
) -> T {
    timeout(Duration::from_secs(6), async {
        tokio::select! {
            result = manager.start_lifecycle() => panic!("lifecycle monitor stopped early: {result:?}"),
            result = manager.start_usage() => panic!("usage monitor stopped early: {result:?}"),
            value = condition => value,
        }
    })
    .await
    .expect("monitors did not reach the expected state")
}

async fn wait_for_file(path: &Path) -> Vec<u8> {
    loop {
        match fs::read(path).await {
            Ok(bytes) if !bytes.is_empty() => return bytes,
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => panic!("cannot read {}: {error}", path.display()),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn read_usage(dir: &Path) -> JobUsage {
    serde_json::from_slice(&wait_for_file(&dir.join("usage.json")).await).unwrap()
}

fn assert_outcome(result: &JobResult, outcome: JobOutcome) {
    assert_eq!(result.outcome().unwrap(), outcome);
}

async fn recover_job<C: Containerd>(manager: &JobManager<C, Graphs>, job_id: Uuid) -> Job {
    let mut jobs = manager.recover().await.unwrap();
    assert_eq!(jobs.len(), 1);
    let job = jobs.pop().unwrap();
    assert_eq!(job.uuid, job_id);
    job
}

#[tokio::test]
async fn mounts_job_asset_links_read_only_from_the_configured_root() {
    let mut workflow = Workflow::new(false);
    workflow.settings.asset_dir = workflow._root.path().join("asset-volume");
    let asset_id = Uuid::new_v4();
    let file_name = "scripts/main.lua";
    workflow.graph.0[0].nodes[0].assets.push(AssetRef {
        asset_id,
        file_name: file_name.into(),
    });
    let script = b"return 1";
    let cache = workflow
        .settings
        .asset_dir
        .join("cache")
        .join(asset_id.to_string());
    fs::create_dir_all(&cache).await.unwrap();
    fs::write(
        cache.join(format!("{:x}.blob", Sha256::digest(script))),
        script,
    )
    .await
    .unwrap();
    let provider = FakeProvider::default();
    let manager = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "true");
    manager.start(job.clone()).await;

    let spec = provider.0.lock().unwrap().prepared[0].clone();
    let mount = spec
        .files
        .iter()
        .find(|file| file.target == "/data/assets")
        .unwrap();
    let aliases = workflow
        .settings
        .asset_dir
        .join("jobs")
        .join(job.uuid.to_string());
    assert_eq!(mount.source, aliases.canonicalize().unwrap());
    assert!(mount.read_only);
    assert_eq!(fs::read(aliases.join(file_name)).await.unwrap(), script);
    assert!(!workflow.data_dir(job.uuid).join("asset").exists());
    assert!(
        provider
            .container(&spec.name)
            .inspect()
            .await
            .unwrap()
            .is_running()
    );
}

#[tokio::test]
async fn yield_and_resume_isolate_job_data_and_keep_unarchived_files() {
    let workflow = Workflow::new(true);
    let provider = FakeProvider::default();
    let usage = example_usage();
    provider.0.lock().unwrap().usage = Some(usage.clone());
    let manager = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "true");
    let dir = workflow.job_dir(job.uuid);
    let data = workflow.data_dir(job.uuid);

    manager.start(job.clone()).await;
    let spec = provider.0.lock().unwrap().prepared[0].clone();
    assert_eq!(spec.image, "fixture-image");
    assert_eq!(spec.files.len(), 4);
    for (target, source, read_only) in [
        ("/rayflow", dir.clone(), true),
        ("/data/inputs", data.join("inputs"), true),
        ("/data/outputs", data.join("outputs"), false),
        ("/data/assets", data.join("asset"), true),
    ] {
        let mount = spec
            .files
            .iter()
            .find(|mount| mount.target == target)
            .unwrap();
        assert_eq!(mount.source, source.canonicalize().unwrap());
        assert_eq!(mount.read_only, read_only);
    }
    assert!(data.join("asset").read_dir().unwrap().next().is_none());
    assert!(!workflow.settings.asset_dir.exists());
    let pinned: DAG =
        serde_json::from_slice(&fs::read(dir.join("graph.json")).await.unwrap()).unwrap();
    assert_eq!(pinned.version, workflow.run.version);
    fs::write(data.join("outputs/result.txt"), b"checkpoint")
        .await
        .unwrap();
    assert!(
        provider
            .container(&spec.name)
            .inspect()
            .await
            .unwrap()
            .is_running()
    );

    let summary = watch_until(&manager, async {
        wait_for_file(&dir.join("usage/checkpoint.jsonl")).await;
        manager.stop(job.uuid, StopReason::Yielded).await.unwrap();
        read_usage(&dir).await.summary.unwrap()
    })
    .await;
    let result = manager.result(job.uuid).await.unwrap().unwrap();
    assert_outcome(&result, JobOutcome::Yielded);
    assert_eq!(result.exit_code, 0);
    assert!(result.duration_ms.is_some());
    assert_eq!(summary.latest, usage);
    drop(manager);

    let manager = workflow.open(provider.clone()).await;
    assert!(manager.recover().await.unwrap().is_empty());
    assert_eq!(manager.result(job.uuid).await.unwrap(), Some(result));
    assert!(data.join("outputs/result.txt").exists());
    assert!(dir.join("usage/checkpoint.jsonl").exists());
    assert_eq!(provider.0.lock().unwrap().stops, 1);

    let resumed = workflow.claim("fixture-image", "true");
    let resumed_dir = workflow.job_dir(resumed.uuid);
    let resumed_data = workflow.data_dir(resumed.uuid);
    assert_ne!(resumed.uuid, job.uuid);
    assert_eq!(resumed.node_run_id, job.node_run_id);
    manager.start(resumed.clone()).await;
    assert!(!resumed_data.join("outputs/result.txt").exists());
    watch_until(
        &manager,
        wait_for_file(&resumed_dir.join("usage/checkpoint.jsonl")),
    )
    .await;
    drop(manager);
    let finished_at = provider.exit(&format!("rayflow-{}", resumed.uuid), 0);

    let manager = workflow.open(provider).await;
    manager
        .start(recover_job(&manager, resumed.uuid).await)
        .await;
    let result = manager.result(resumed.uuid).await.unwrap().unwrap();
    assert_outcome(&result, JobOutcome::Succeeded);
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.finished_at, Some(finished_at));
    let summary = watch_until(&manager, read_usage(&resumed_dir)).await;
    assert_eq!(summary.summary.unwrap().latest, usage);
}

#[tokio::test]
async fn restarting_during_execution_preserves_container_and_recovers_failure() {
    let mut workflow = Workflow::new(false);
    let gpu_cdi_device = "vendor.example/gpu=1";
    workflow.settings.gpu_devices = ["vendor.example/gpu=0", gpu_cdi_device]
        .into_iter()
        .map(|device| GpuDevice {
            cdi_device: device.into(),
            memory_bytes: 1024,
        })
        .collect();
    let provider = FakeProvider::default();
    let usage = example_usage();
    provider.0.lock().unwrap().usage = Some(usage.clone());
    let manager = workflow.open(provider.clone()).await;
    let mut job = workflow.claim("fixture-image", "exit 7");
    job.required_gpu_memory_bytes = 1024;
    job.assigned_gpu_cdi_device = Some(gpu_cdi_device.into());
    let dir = workflow.job_dir(job.uuid);
    let output = workflow.data_dir(job.uuid).join("outputs/checkpoint.txt");

    manager.start(job.clone()).await;
    fs::write(&output, b"pending report").await.unwrap();
    watch_until(&manager, wait_for_file(&dir.join("usage/checkpoint.jsonl"))).await;
    drop(manager);

    let manager = workflow.open(provider.clone()).await;
    let recovered = recover_job(&manager, job.uuid).await;
    assert_eq!(recovered.expired_at, job.expired_at);
    assert_eq!(
        recovered.assigned_gpu_cdi_device,
        job.assigned_gpu_cdi_device
    );
    let mut redelivery = job.clone();
    redelivery.expired_at = Some(Timestamp::now());
    let start = manager.start(redelivery);
    assert_eq!(manager.active_job_ids(), vec![job.uuid]);
    tokio::join!(start, manager.start(recovered));
    assert_eq!(provider.0.lock().unwrap().starts, 1);

    let sample_started = Arc::new(Notify::new());
    {
        let mut state = provider.0.lock().unwrap();
        state.sample_started = Some(sample_started.clone());
        state.sample_fails = true;
    }
    watch_until(&manager, sample_started.notified()).await;
    let finished_at = provider.exit(&format!("rayflow-{}", job.uuid), 7);
    drop(manager);

    let manager = workflow.open(provider).await;
    let recovered = recover_job(&manager, job.uuid).await;
    assert_eq!(
        recovered.assigned_gpu_cdi_device.as_deref(),
        Some(gpu_cdi_device)
    );
    manager.start(recovered).await;
    let result = manager.result(job.uuid).await.unwrap().unwrap();
    assert_outcome(&result, JobOutcome::Failed);
    assert_eq!(result.exit_code, 7);
    assert_eq!(result.finished_at, Some(finished_at));
    assert!(result.duration_ms.is_some());
    let summary = watch_until(&manager, read_usage(&dir)).await;
    assert_eq!(summary.summary.unwrap().latest, usage);
    assert!(output.exists());
}

#[tokio::test]
async fn recovering_a_lost_create_reply_starts_the_same_container() {
    let workflow = Workflow::new(false);
    let provider = FakeProvider::default();
    provider.0.lock().unwrap().lose_create_reply = true;
    let manager = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "sleep 30");

    manager.start(job.clone()).await;

    assert!(workflow.job_dir(job.uuid).join("container.json").exists());
    assert!(manager.result(job.uuid).await.unwrap().is_none());
    let state = provider.0.lock().unwrap();
    assert_eq!(state.creates, 1);
    assert_eq!(state.starts, 1);
}

#[tokio::test]
async fn recovery_keeps_the_claim_when_container_inspection_is_unavailable() {
    let workflow = Workflow::new(false);
    let provider = FakeProvider::default();
    let manager = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "sleep 30");
    manager.start(job.clone()).await;
    drop(manager);

    let manager = workflow.open(provider.clone()).await;
    let prior_inspections = {
        let mut state = provider.0.lock().unwrap();
        state.fail_inspect = true;
        state.inspections
    };
    let recovered = recover_job(&manager, job.uuid).await;
    {
        let mut state = provider.0.lock().unwrap();
        assert_eq!(state.inspections, prior_inspections);
        state.fail_inspect = false;
    }

    manager.start(recovered).await;
    assert!(manager.result(job.uuid).await.unwrap().is_none());
    assert_eq!(provider.0.lock().unwrap().starts, 1);
}

#[tokio::test]
async fn recovering_an_expired_unrecorded_container_keeps_its_identity_for_cleanup() {
    let workflow = Workflow::new(false);
    let provider = FakeProvider::default();
    let manager = workflow.open(provider.clone()).await;
    let mut job = workflow.claim("fixture-image", "sleep 30");
    job.expired_at = Some(Timestamp::now());
    let dir = workflow.job_dir(job.uuid);
    fs::create_dir(&dir).await.unwrap();
    let mut persisted = serde_json::to_value(&job).unwrap();
    persisted["node_config"]
        .as_object_mut()
        .unwrap()
        .remove("name");
    fs::write(
        dir.join("job.json"),
        serde_json::to_vec(&persisted).unwrap(),
    )
    .await
    .unwrap();
    provider.create(provider.spec(&job).unwrap()).await.unwrap();

    let recovered = recover_job(&manager, job.uuid).await;
    assert_eq!(
        recovered.node_config.get().as_ref().unwrap().name,
        "default"
    );
    manager.start(recovered).await;

    assert!(dir.join("container.json").exists());
    let result = manager.result(job.uuid).await.unwrap().unwrap();
    assert_eq!(result.exit_code, 124);
    let state = provider.0.lock().unwrap();
    assert_eq!(state.creates, 1);
    assert_eq!(state.starts, 0);
    assert_eq!(state.kills, 1);
}

#[tokio::test]
async fn summarizes_checkpoint_log_after_recovery() {
    let workflow = Workflow::new(false);
    let provider = FakeProvider::default();
    let manager = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "sleep 30");
    let dir = workflow.job_dir(job.uuid);
    manager.start(job.clone()).await;
    let started_at = provider
        .container(&format!("rayflow-{}", job.uuid))
        .inspect()
        .await
        .unwrap()
        .started_at()
        .unwrap();
    let checkpoint = |seconds: u64, cpu: u64, memory: u64| Usage {
        sampled_at: started_at
            .checked_add(Duration::from_secs(seconds))
            .unwrap(),
        cpu_usage_ns: Some(cpu),
        memory_usage_bytes: Some(memory),
        memory_working_set_bytes: None,
        memory_limit_bytes: None,
    };
    let first = checkpoint(0, 2_000_000_000, 100);
    let middle = checkpoint(2, 5_000_000_000, 200);
    let last = checkpoint(5, 8_000_000_000, 50);
    fs::create_dir(dir.join("usage")).await.unwrap();
    let mut lines = [last.clone(), first, middle]
        .into_iter()
        .map(|sample| serde_json::to_string(&sample).unwrap())
        .collect::<Vec<_>>();
    lines.push(String::new());
    fs::write(dir.join("usage/checkpoint.jsonl"), lines.join("\n"))
        .await
        .unwrap();
    drop(manager);

    provider.0.lock().unwrap().containers.insert(
        format!("rayflow-{}", job.uuid),
        ContainerState::Exited {
            exit_code: 0,
            started_at: Some(started_at),
            finished_at: Some(last.sampled_at),
        },
    );
    let manager = workflow.open(provider).await;
    manager.start(recover_job(&manager, job.uuid).await).await;
    let summary = watch_until(&manager, read_usage(&dir))
        .await
        .summary
        .unwrap();
    assert_eq!(summary.latest, last);
    assert_eq!(summary.peak_cpu_millicores, Some(1500));
    assert_eq!(
        summary.cpu_millicores_percentiles,
        Some(UsagePercentiles {
            p50: 1000,
            p90: 1500,
            p95: 1500,
            p99: 1500,
        })
    );
    assert_eq!(summary.total_cpu_usage_ns, Some(8_000_000_000));
    assert_eq!(summary.peak_memory_usage_bytes, Some(200));
    assert_eq!(
        summary.memory_usage_bytes_percentiles,
        Some(UsagePercentiles {
            p50: 100,
            p90: 200,
            p95: 200,
            p99: 200,
        })
    );
    assert_eq!(summary.memory_working_set_bytes_percentiles, None);
    assert_eq!(summary.total_memory_usage_byte_seconds, Some(675));
}

#[tokio::test]
async fn start_checks_the_current_result_under_the_job_lock() {
    let workflow = Workflow::new(false);
    let provider = FakeProvider::default();
    let manager = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "sleep 30");
    let dir = workflow.job_dir(job.uuid);
    let result_path = dir.join("result.json");
    manager.start(job.clone()).await;
    drop(manager);

    let manager = workflow.open(provider.clone()).await;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("lock"))
        .unwrap();
    lock.lock().unwrap();
    let mut start = Box::pin(manager.start(job.clone()));
    assert!(
        timeout(Duration::from_millis(50), &mut start)
            .await
            .is_err()
    );
    let finished_at = provider.exit(&format!("rayflow-{}", job.uuid), 143);
    let legacy = json!({
        "exit_code": 0,
        "outcome": "yield",
        "finished_at": finished_at,
        "usage": {"sampled_at": finished_at, "total_cpu_usage_ns": 1234},
    });
    fs::write(result_path, legacy.to_string()).await.unwrap();
    drop(lock);
    start.await;

    let result = manager.result(job.uuid).await.unwrap().unwrap();
    assert_outcome(&result, JobOutcome::Yielded);
    assert_eq!(result.exit_code, 0);
    assert_eq!(provider.0.lock().unwrap().starts, 1);
    let usage = watch_until(&manager, read_usage(&dir)).await;
    assert_eq!(usage.summary.unwrap().total_cpu_usage_ns, Some(1234));
}

#[tokio::test]
async fn expired_running_job_is_killed_when_events_are_unavailable() {
    let workflow = Workflow::new(false);
    let provider = FakeProvider::default();
    let usage = example_usage();
    provider.0.lock().unwrap().usage = Some(usage.clone());
    let manager = workflow.open(provider.clone()).await;
    let mut job = workflow.claim("fixture-image", "sleep 30");
    job.expired_at = Some(
        Timestamp::now()
            .checked_add(Duration::from_secs(2))
            .unwrap(),
    );
    let dir = workflow.job_dir(job.uuid);
    manager.start(job.clone()).await;
    assert!(
        provider
            .container(&format!("rayflow-{}", job.uuid))
            .inspect()
            .await
            .unwrap()
            .is_running()
    );

    let summary = watch_until(&manager, read_usage(&dir)).await;
    let result = manager.result(job.uuid).await.unwrap().unwrap();
    assert_outcome(&result, JobOutcome::Failed);
    assert_eq!(result.exit_code, 124);
    assert_eq!(summary.summary.unwrap().latest, usage);
    assert_eq!(provider.0.lock().unwrap().kills, 1);
}

#[tokio::test]
async fn usage_includes_a_sample_received_after_the_result_is_saved() {
    let workflow = Workflow::new(false);
    let provider = FakeProvider::default();
    let manager = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "sleep 30");
    let dir = workflow.job_dir(job.uuid);
    manager.start(job.clone()).await;
    let container = provider.container(&format!("rayflow-{}", job.uuid));

    let sample = example_usage();
    let opened = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    provider.0.lock().unwrap().usage_stream = Some(Box::pin(stream::once({
        let opened = opened.clone();
        let release = release.clone();
        let sample = sample.clone();
        async move {
            opened.notify_one();
            release.notified().await;
            Ok(sample)
        }
    })));

    let usage = watch_until(&manager, async {
        opened.notified().await;
        manager
            .stop(job.uuid, StopReason::Terminated)
            .await
            .unwrap();
        let result = manager.result(job.uuid).await.unwrap().unwrap();
        assert_outcome(&result, JobOutcome::Terminated);
        assert!(!dir.join("usage.json").exists());
        assert!(matches!(
            container.inspect().await.unwrap(),
            ContainerState::Exited { .. }
        ));
        release.notify_one();
        let usage = read_usage(&dir).await;
        loop {
            let metadata: serde_json::Value =
                serde_json::from_slice(&fs::read(dir.join("container.json")).await.unwrap())
                    .unwrap();
            assert_eq!(metadata["id"], container.id());
            if metadata["removed"] == true {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        usage
    })
    .await;
    assert_eq!(
        container.inspect().await.unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert!(dir.join("result.json").exists());
    assert!(dir.join("usage.json").exists());
    assert!(!dir.join("usage.json.upload").exists());
    assert_eq!(usage.summary.unwrap().latest, sample);
}

#[tokio::test]
async fn checkpoint_write_failure_retains_only_a_bounded_batch_for_retry() {
    let workflow = Workflow::new(false);
    let provider = FakeProvider::default();
    let manager = workflow.open(provider.clone()).await;
    let job = workflow.claim("fixture-image", "sleep 30");
    let dir = workflow.job_dir(job.uuid);
    manager.start(job.clone()).await;
    fs::write(dir.join("usage"), b"checkpoint directory unavailable")
        .await
        .unwrap();
    let consumed = Arc::new(Notify::new());
    let samples = (0..96).map(|counter| {
        Ok(Usage {
            cpu_usage_ns: Some(counter),
            ..example_usage()
        })
    });
    provider.0.lock().unwrap().usage_stream =
        Some(Box::pin(stream::iter(samples).chain(stream::once({
            let consumed = consumed.clone();
            async move {
                consumed.notify_one();
                pending().await
            }
        }))));

    let bytes = watch_until(&manager, async {
        consumed.notified().await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        fs::remove_file(dir.join("usage")).await.unwrap();
        manager
            .stop(job.uuid, StopReason::Terminated)
            .await
            .unwrap();
        read_usage(&dir).await;
        fs::read(dir.join("usage/checkpoint.jsonl")).await.unwrap()
    })
    .await;
    let counters: Vec<u64> = serde_json::Deserializer::from_slice(&bytes)
        .into_iter::<Usage>()
        .map(|sample| sample.unwrap().cpu_usage_ns.unwrap())
        .collect();
    assert_eq!(counters, (64..96).collect::<Vec<_>>());
}

#[tokio::test]
async fn stalled_usage_sampling_does_not_delay_stopping_a_job() {
    let workflow = Workflow::new(false);
    let provider = FakeProvider::default();
    let sample_started = Arc::new(Notify::new());
    provider.0.lock().unwrap().sample_started = Some(sample_started.clone());
    let manager = workflow.open(provider).await;
    let job = workflow.claim("fixture-image", "sleep 30");
    let dir = workflow.job_dir(job.uuid);
    manager.start(job.clone()).await;
    let usage = watch_until(&manager, async {
        sample_started.notified().await;
        timeout(
            Duration::from_millis(300),
            manager.stop(job.uuid, StopReason::Terminated),
        )
        .await
        .expect("result must be independent of sampling")
        .unwrap();
        let result = manager.result(job.uuid).await.unwrap().unwrap();
        assert_outcome(&result, JobOutcome::Terminated);
        read_usage(&dir).await
    })
    .await;
    assert_eq!(usage.summary, None);
}

#[tokio::test]
#[ignore = "requires a Docker daemon and a local shell image in RAYFLOW_TEST_DOCKER_IMAGE or busybox:latest"]
async fn docker_exit_statuses_are_recovered() {
    let image =
        std::env::var("RAYFLOW_TEST_DOCKER_IMAGE").unwrap_or_else(|_| "busybox:latest".into());
    let workflow = Workflow::new(false);
    let docker = DockerContainerd::connect_with_defaults().unwrap();
    docker.docker().inspect_image(&image).await.unwrap();
    let jobs = [(JobOutcome::Succeeded, 0), (JobOutcome::Failed, 7)].map(|(outcome, exit_code)| {
        let command =
            format!("while [ ! -f /data/outputs/exit ]; do sleep 1; done; exit {exit_code}");
        (workflow.claim(&image, &command), outcome, exit_code)
    });
    let docker_for_test = docker.clone();
    let test_jobs = jobs.clone();
    let result = AssertUnwindSafe(async move {
        let mut manager = workflow.open(docker_for_test.clone()).await;
        for (job, outcome, exit_code) in test_jobs {
            manager.start(job.clone()).await;
            let container = docker_for_test.container(&format!("rayflow-{}", job.uuid));
            assert!(container.inspect().await.unwrap().is_running());
            drop(manager);
            fs::write(workflow.data_dir(job.uuid).join("outputs/exit"), b"")
                .await
                .unwrap();
            let status = timeout(Duration::from_secs(10), container.wait())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(status.exit_code, exit_code);
            assert!(matches!(
                container.inspect().await.unwrap(),
                ContainerState::Exited {
                    started_at: Some(_),
                    ..
                }
            ));

            manager = workflow.open(docker_for_test.clone()).await;
            manager.start(recover_job(&manager, job.uuid).await).await;
            let result = manager.result(job.uuid).await.unwrap().unwrap();
            assert_outcome(&result, outcome);
            assert_eq!(result.exit_code, exit_code);
            assert!(result.duration_ms.is_some());
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
