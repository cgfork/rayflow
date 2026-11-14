use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use agent::{
    Agentd, Agentic,
    client::Client,
    containerd::{Container, ContainerEvent, ContainerState, Containerd, ExitStatus},
    errors::Error,
    graphfs::GraphFs,
    jobfs::JobFs,
    settings::Settings,
};
use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::post};
use event::task::Task;
use futures_util::Stream;
use graph::types::{DAG, Node};
use jiff::Timestamp;
use scheduler::{Job, ResourceCapacity};
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::{fs, net::TcpListener, sync::mpsc, time::timeout};
use uuid::Uuid;

#[derive(Clone)]
struct IdleProvider;

#[derive(Clone)]
struct IdleContainer;

impl Container for IdleContainer {
    fn id(&self) -> &str {
        "idle"
    }

    async fn start(&self) -> Result<(), Error> {
        unreachable!()
    }

    async fn wait(&self) -> Result<ExitStatus, Error> {
        unreachable!()
    }

    async fn inspect(&self) -> Result<ContainerState, Error> {
        unreachable!()
    }

    async fn stop(&self) -> Result<(), Error> {
        unreachable!()
    }

    async fn kill(&self) -> Result<(), Error> {
        unreachable!()
    }

    async fn remove(&self) -> Result<(), Error> {
        unreachable!()
    }
}

impl Containerd for IdleProvider {
    type Container = IdleContainer;
    type Spec<'a> = ();

    fn spec<'a>(&'a self, _job: &'a Job) -> Result<Self::Spec<'a>, Error> {
        Ok(())
    }

    async fn prepare(&self, _spec: Self::Spec<'_>) -> Result<(), Error> {
        Ok(())
    }

    async fn create(&self, _spec: Self::Spec<'_>) -> Result<Self::Container, Error> {
        unreachable!()
    }

    fn container(&self, _id: &str) -> Self::Container {
        IdleContainer
    }

    fn events(&self) -> Pin<Box<dyn Stream<Item = Result<ContainerEvent, Error>> + Send>> {
        Box::pin(futures_util::stream::pending())
    }
}

#[derive(Clone)]
struct CompletingProvider(Arc<AtomicUsize>);

#[derive(Clone)]
struct CompletingContainer(Arc<AtomicUsize>);

impl Container for CompletingContainer {
    fn id(&self) -> &str {
        "complete"
    }

    async fn start(&self) -> Result<(), Error> {
        self.0.store(1, Ordering::SeqCst);
        Ok(())
    }

    async fn wait(&self) -> Result<ExitStatus, Error> {
        Ok(ExitStatus {
            exit_code: 0,
            finished_at: None,
        })
    }

    async fn inspect(&self) -> Result<ContainerState, Error> {
        if self.0.load(Ordering::SeqCst) == 0 {
            Ok(ContainerState::Created)
        } else {
            Ok(ContainerState::Exited {
                exit_code: 0,
                finished_at: Some(Timestamp::now()),
            })
        }
    }

    async fn stop(&self) -> Result<(), Error> {
        unreachable!()
    }

    async fn kill(&self) -> Result<(), Error> {
        unreachable!()
    }

    async fn remove(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl Containerd for CompletingProvider {
    type Container = CompletingContainer;
    type Spec<'a> = ();

    fn spec<'a>(&'a self, _job: &'a Job) -> Result<Self::Spec<'a>, Error> {
        Ok(())
    }

    async fn prepare(&self, _spec: Self::Spec<'_>) -> Result<(), Error> {
        Ok(())
    }

    async fn create(&self, _spec: Self::Spec<'_>) -> Result<Self::Container, Error> {
        Ok(CompletingContainer(self.0.clone()))
    }

    fn container(&self, _id: &str) -> Self::Container {
        CompletingContainer(self.0.clone())
    }

    fn events(&self) -> Pin<Box<dyn Stream<Item = Result<ContainerEvent, Error>> + Send>> {
        Box::pin(futures_util::stream::pending())
    }
}

#[tokio::test]
async fn polled_job_persists_node_starts_and_reports() {
    let root = tempdir().unwrap();
    let workdir = root.path().join("jobs");
    let task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 1);
    let job = Job::new(&task, Uuid::new_v4(), 1, 1);
    let node = Node {
        id: task.node_id,
        name: "job node".into(),
        node_type: "shell".into(),
        settings: Default::default(),
        inputs: vec![],
        outputs: vec![],
    };
    let graphfs = GraphFs::from_workdir(&workdir);
    graphfs
        .write_graph(DAG {
            id: task.graph_id,
            name: "test".into(),
            version: 1,
            nodes: vec![node.clone()],
            edges: vec![],
            node_count: 1,
            ..DAG::default()
        })
        .await
        .unwrap();
    let now = Timestamp::now();
    let mut value = serde_json::to_value(&job).unwrap();
    value["run"] = json!({
        "id": 1, "uuid": job.run_id, "data_id": task.data_id,
        "graph_id": task.graph_id, "version": 1, "revision": 1,
        "created_at": now, "updated_at": now
    });
    value["node_run"] = json!({
        "id": task.node_run_id, "run_id": job.run_id,
        "data_id": task.data_id, "graph_id": task.graph_id,
        "version": 1, "node_id": task.node_id, "revision": 1,
        "status": "Running", "created_at": now, "updated_at": now
    });
    value["node_config"] = json!({
        "id": 1, "node_id": task.node_id, "version": 1,
        "annotation_match": "", "config": {}, "lease": 30000,
        "required_cpu_cores": 1, "required_memory_bytes": 1024,
        "created_at": now, "updated_at": now, "created_by": "", "updated_by": ""
    });
    let job: Job = serde_json::from_value(value).unwrap();

    let (requests, mut received) = mpsc::unbounded_channel::<(String, Value)>();
    let polls = Arc::new(AtomicUsize::new(0));
    let polled_job = job.clone();
    let router = Router::new()
        .route(
            "/jobs/poll",
            post({
                let requests = requests.clone();
                move |Json(body): Json<Value>| {
                    let requests = requests.clone();
                    let polls = polls.clone();
                    let job = polled_job.clone();
                    async move {
                        requests.send(("poll".into(), body)).unwrap();
                        if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                            Json(json!({"jobs": [job]}))
                        } else {
                            Json(json!({"jobs": []}))
                        }
                    }
                }
            }),
        )
        .route(
            "/job/succeed",
            post(move |Json(body): Json<Value>| {
                let requests = requests.clone();
                async move {
                    requests.send(("succeed".into(), body)).unwrap();
                    StatusCode::NO_CONTENT
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut settings = Settings::new(
        Uuid::new_v4(),
        url,
        &workdir,
        ResourceCapacity {
            cpu_cores: 1,
            memory_bytes: 1024,
        },
    );
    settings.poll_interval = Duration::from_millis(20);
    let client = Client::new(&settings).unwrap();
    let provider = CompletingProvider(Arc::new(AtomicUsize::new(0)));
    let started = provider.0.clone();
    let daemon = Agentd::open(provider, graphfs, &workdir).await.unwrap();
    let agentic = Agentic::with_agent(daemon, client, settings).unwrap();
    let running = agentic.start();
    tokio::pin!(running);

    let job_fs = JobFs::new(&workdir, job.uuid);
    let reported = timeout(Duration::from_secs(5), async {
        while let Some((action, body)) = received.recv().await {
            if action == "succeed" {
                assert_eq!(body["job_id"], json!(job.uuid));
                assert_eq!(job_fs.node().await.unwrap(), node);
                assert_eq!(started.load(Ordering::SeqCst), 1);
                break;
            }
        }
        while job_fs.dir().join("result.json").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    tokio::select! {
        result = &mut running => panic!("agentic stopped early: {result:?}"),
        result = reported => result.unwrap(),
    }
    timeout(Duration::from_secs(2), async {
        let (running, shutdown) = tokio::join!(&mut running, agentic.shutdown());
        running.unwrap();
        shutdown.unwrap();
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test]
async fn recovered_result_is_retried_and_acknowledged() {
    let root = tempdir().unwrap();
    let workdir = root.path().join("jobs");
    let task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 1);
    let job = Job::new(&task, Uuid::new_v4(), 1, 1);
    let mut value = serde_json::to_value(&job).unwrap();
    let now = Timestamp::now();
    value["run"] = json!({"id":1,"uuid":job.run_id,"data_id":task.data_id,"graph_id":task.graph_id,"version":1,"revision":3,"created_at":now,"updated_at":now});
    value["node_run"] = json!({"id":task.node_run_id,"run_id":job.run_id,"data_id":task.data_id,"graph_id":task.graph_id,"version":1,"node_id":task.node_id,"revision":3,"status":"Running","created_at":now,"updated_at":now});
    let job: Job = serde_json::from_value(value).unwrap();
    let file_fs = agent::filefs::FileFs::new(workdir.join("data"), job.run_id, task.node_id, 3);
    file_fs.prepare().await.unwrap();
    fs::write(file_fs.outputs_dir().join("result.txt"), "hello")
        .await
        .unwrap();
    let job_dir = workdir.join(job.uuid.to_string());
    fs::create_dir_all(&job_dir).await.unwrap();
    fs::write(job_dir.join("job.json"), serde_json::to_vec(&job).unwrap())
        .await
        .unwrap();
    fs::write(job_dir.join("result.json"), br#"{"exit_code":0}"#)
        .await
        .unwrap();
    fs::write(
        job_dir.join("node.json"),
        serde_json::to_vec(&Node {
            id: task.node_id,
            name: "job node".into(),
            node_type: "shell".into(),
            settings: Default::default(),
            inputs: vec![],
            outputs: vec![graph::types::Output {
                file_name: "result.txt".into(),
                kind: graph::types::Kind::File,
                multiple: false,
            }],
        })
        .unwrap(),
    )
    .await
    .unwrap();

    let (requests, mut received) = mpsc::unbounded_channel::<(String, Value)>();
    let attempts = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let object_url = format!("{url}/object");
    let upload_requests = requests.clone();
    let commit_requests = requests.clone();
    let put_attempts = Arc::new(AtomicUsize::new(0));
    let catalog_uuid = Uuid::new_v4();
    let graph = DAG {
        id: task.graph_id,
        name: "flow".into(),
        version: 1,
        ..Default::default()
    };
    let router = Router::new()
        .route("/graphs/{id}", axum::routing::get(move || { let graph = graph.clone(); async move { Json(graph) } }))
        .route("/catalog/prepare", post(move |Json(body): Json<Value>| {
            let object_url = object_url.clone();
            async move {
                assert!(body.get("catalog_uuid").is_none());
                Json(json!({
                    "catalog": catalog::model::Catalog::new(catalog_uuid, "/result.txt", "s3://bucket/result.txt", 5, "txt"),
                    "presigned": {"url":object_url,"operation":"upload","headers":{},"expires_at":Timestamp::now() + Duration::from_secs(60)}
                }))
            }
        }))
        .route("/object", axum::routing::put(move |bytes: axum::body::Bytes| {
            let requests = upload_requests.clone();
            let attempts = put_attempts.clone();
            async move {
                assert_eq!(bytes.as_ref(), b"hello");
                requests.send(("put".into(), Value::Null)).unwrap();
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 { StatusCode::SERVICE_UNAVAILABLE } else { StatusCode::OK }
            }
        }))
        .route("/catalog/commit", post(move |Json(body): Json<Value>| {
            let requests = commit_requests.clone();
            async move {
                assert_eq!(body["catalog_uuid"], catalog_uuid.to_string());
                requests.send(("commit".into(), body)).unwrap();
                StatusCode::NO_CONTENT
            }
        }))
        .route(
            "/jobs/poll",
            post({
                let requests = requests.clone();
                move |Json(body): Json<Value>| {
                    let requests = requests.clone();
                    async move {
                        requests.send(("poll".into(), body)).unwrap();
                        Json(json!({"jobs": []}))
                    }
                }
            }),
        )
        .route(
            "/job/succeed",
            post(move |Json(body): Json<Value>| {
                let requests = requests.clone();
                let attempts = attempts.clone();
                async move {
                    requests.send(("succeed".into(), body)).unwrap();
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else {
                        StatusCode::NO_CONTENT.into_response()
                    }
                }
            }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut settings = Settings::new(
        Uuid::new_v4(),
        url,
        &workdir,
        ResourceCapacity {
            cpu_cores: 1,
            memory_bytes: 1024,
        },
    );
    settings.poll_interval = Duration::from_millis(20);
    let client = Client::new(&settings).unwrap();
    let daemon = Agentd::open(IdleProvider, GraphFs::from_workdir(&workdir), &workdir)
        .await
        .unwrap();
    let agentic = Agentic::with_agent(daemon, client, settings).unwrap();
    let running = agentic.start();
    tokio::pin!(running);

    let mut puts = 0;
    let mut committed = false;
    let mut reported = 0;
    let mut polled = false;
    let reports = timeout(Duration::from_secs(5), async {
        while reported < 2 || !polled {
            let (action, body) = received.recv().await.unwrap();
            if action == "poll" && !polled {
                assert!(
                    body["active_jobs"]
                        .as_array()
                        .unwrap()
                        .contains(&json!(job.uuid))
                );
                polled = true;
            }
            if action == "put" {
                puts += 1;
            }
            if action == "commit" {
                committed = true;
            }
            if action == "succeed" {
                assert!(committed, "outputs must be committed before success");
                assert_eq!(puts, 2, "failed uploads must retry before reporting");
                assert_eq!(body["job_id"], json!(job.uuid));
                reported += 1;
                if reported == 1 {
                    assert!(job_dir.join("result.json").is_file());
                }
            }
        }
        while job_dir.join("result.json").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    tokio::select! {
        result = &mut running => panic!("agentic stopped early: {result:?}"),
        result = reports => result.unwrap(),
    }

    timeout(Duration::from_secs(2), async {
        let (running, shutdown) = tokio::join!(&mut running, agentic.shutdown());
        running.unwrap();
        shutdown.unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        fs::read(file_fs.outputs_dir().join("result.txt"))
            .await
            .unwrap(),
        b"hello"
    );
    server.abort();
}
