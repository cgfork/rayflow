//! Direct agent polling, resource matching, and claim recovery.
//!
//! Set `DATABASE_URL` to run this test against an isolated MySQL database. The
//! test resets that database and rebuilds its schema from the Toasty models.

use std::time::Duration;

use event::{
    Emit,
    task::Task,
    task_event::{TaskEvent, TaskEventEntry},
};
use graph::types::DAG;
use jiff::Timestamp;
use runtime::command::{NodeRun, Run};
use scheduler::{
    Actuator, AgentScheduler, DEFAULT_JOB_LEASE, Job, JobWrite, ResourceCapacity, Scheduler,
    actuator::{NodeConfig, Status},
    poll_polled, poll_scheduled,
};
use toasty::Db;
use uuid::Uuid;

async fn setup() -> Option<Db> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let mut builder = Db::builder();
    builder.models(scheduler::models());
    let db = builder
        .connect(&url)
        .await
        .expect("connect to DATABASE_URL");
    db.reset_db().await.expect("reset database");
    db.push_schema().await.expect("push schema");
    Some(db)
}

async fn configured_job(
    db: &mut Db,
    run_id: Uuid,
    data_id: Uuid,
    graph_id: Uuid,
    cpu_cores: u32,
    memory_bytes: u64,
) -> (Task, Job) {
    let now = Timestamp::now();
    let node_id = Uuid::new_v4();
    let node_run = NodeRun::create()
        .run_id(run_id)
        .data_id(data_id)
        .graph_id(graph_id)
        .node_id(node_id)
        .revision(1)
        .status("Running".to_owned())
        .created_at(now)
        .updated_at(now)
        .exec(&mut *db)
        .await
        .expect("create node run");
    NodeConfig::create()
        .node_id(node_id)
        .version(1)
        .annotation_match(String::new())
        .config(serde_json::json!({}))
        .required_cpu_cores(cpu_cores)
        .required_memory_bytes(memory_bytes)
        .created_at(now)
        .updated_at(now)
        .created_by(String::new())
        .updated_by(String::new())
        .exec(&mut *db)
        .await
        .expect("create node config");
    let task = Task::new(data_id, graph_id, node_id, node_run.id);
    task.emit(&mut *db).await.expect("emit task");
    let job = Job::create_for_task(&task, db)
        .await
        .expect("create job from task");
    (task, job)
}

async fn poll_jobs_with_lease(
    db: &mut Db,
    agent_id: Uuid,
    capacity: ResourceCapacity,
    maximum: usize,
    lease: Duration,
) -> Result<Vec<Job>, scheduler::Error> {
    let mut tx = db.transaction().await?;
    let (mut polled, reserved) = poll_polled(&mut tx, agent_id, &[], maximum, lease).await?;
    let remaining = ResourceCapacity {
        cpu_cores: capacity.cpu_cores.saturating_sub(reserved.cpu_cores),
        memory_bytes: capacity.memory_bytes.saturating_sub(reserved.memory_bytes),
    };
    let scheduled = poll_scheduled(
        &mut tx,
        agent_id,
        remaining,
        maximum.saturating_sub(polled.len()),
    )
    .await?;
    polled.extend(scheduled);
    tx.commit().await?;
    Ok(polled)
}

async fn poll_jobs(
    db: &mut Db,
    agent_id: Uuid,
    capacity: ResourceCapacity,
    maximum: usize,
) -> Result<Vec<Job>, scheduler::Error> {
    poll_jobs_with_lease(db, agent_id, capacity, maximum, DEFAULT_JOB_LEASE).await
}

#[tokio::test]
async fn direct_poll_matches_resources_reserves_claims_and_recovers_expired_jobs() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run MySQL agent polling tests");
        return;
    };
    let now = Timestamp::now();
    let data_id = Uuid::new_v4();
    let graph_id = Uuid::new_v4();
    let run = Run::create()
        .uuid(Uuid::new_v4())
        .data_id(data_id)
        .graph_id(graph_id)
        .version(1)
        .revision(1)
        .created_at(now)
        .updated_at(now)
        .exec(&mut db)
        .await
        .expect("create run");

    let (task_a, job_a) = configured_job(&mut db, run.uuid, data_id, graph_id, 2, 512).await;
    let (_, job_b) = configured_job(&mut db, run.uuid, data_id, graph_id, 2, 512).await;
    let (_, job_c) = configured_job(&mut db, run.uuid, data_id, graph_id, 1, 1).await;
    assert_eq!(
        (job_a.required_cpu_cores, job_a.required_memory_bytes),
        (2, 512)
    );
    assert_eq!(
        (job_b.required_cpu_cores, job_b.required_memory_bytes),
        (2, 512)
    );
    assert_eq!(
        (job_c.required_cpu_cores, job_c.required_memory_bytes),
        (1, 1)
    );

    let agent_a = Uuid::new_v4();
    let capacity_a = ResourceCapacity {
        cpu_cores: 4,
        memory_bytes: 1024,
    };
    let claimed = poll_jobs(&mut db, agent_a, capacity_a, 10)
        .await
        .expect("poll jobs");
    assert_eq!(
        claimed.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![job_a.id, job_b.id]
    );
    assert!(claimed.iter().all(|job| job.agent_id == Some(agent_a)));
    let redelivered = poll_jobs(&mut db, agent_a, capacity_a, 10).await.unwrap();
    assert_eq!(
        redelivered.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![job_a.id, job_b.id]
    );
    for job in &claimed {
        Job::filter_by_id(job.id)
            .update()
            .status(Status::Pending)
            .exec(&mut db)
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert!(
        poll_jobs(&mut db, agent_a, capacity_a, 10)
            .await
            .unwrap()
            .is_empty(),
        "the active claims consume all of the agent's resource budget"
    );
    let renewed = Job::filter_by_id(job_a.id).get(&mut db).await.unwrap();
    assert!(renewed.polled_at.unwrap() >= claimed[0].polled_at.unwrap());

    let agent_b = Uuid::new_v4();
    let claimed_c = poll_jobs(
        &mut db,
        agent_b,
        ResourceCapacity {
            cpu_cores: 1,
            memory_bytes: 1,
        },
        10,
    )
    .await
    .expect("claim remaining fitting job");
    assert_eq!(claimed_c.len(), 1);
    assert_eq!(claimed_c[0].id, job_c.id);

    let expired_at = Timestamp::now() - Duration::from_secs(2);
    Job::filter_by_id(job_a.id)
        .update()
        .polled_at(Some(expired_at))
        .exec(&mut db)
        .await
        .expect("expire one claim");
    assert_eq!(
        AgentScheduler::new()
            .with_lease(Duration::from_secs(1))
            .schedule(&mut db)
            .await
            .unwrap(),
        1
    );
    let expired = Job::filter_by_id(job_a.id).get(&mut db).await.unwrap();
    assert_eq!(expired.status, Status::Scheduled);
    assert!(expired.agent_id.is_none());
    assert!(expired.polled_at.is_none());

    let agent_c = Uuid::new_v4();
    let agent_d = Uuid::new_v4();
    let mut db_c = db.clone();
    let mut db_d = db.clone();
    let (left, right) = tokio::join!(
        poll_jobs(
            &mut db_c,
            agent_c,
            ResourceCapacity {
                cpu_cores: 2,
                memory_bytes: 512
            },
            10,
        ),
        poll_jobs(
            &mut db_d,
            agent_d,
            ResourceCapacity {
                cpu_cores: 2,
                memory_bytes: 512
            },
            10,
        ),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.len() + right.len(), 1);
    let owner = if left.is_empty() { agent_d } else { agent_c };
    Actuator::<DAG>::settle(&mut db, &TaskEventEntry::new(&task_a, TaskEvent::Pend))
        .await
        .unwrap();
    assert_eq!(
        Job::filter_by_id(job_a.id)
            .get(&mut db)
            .await
            .unwrap()
            .status,
        Status::Polled
    );
    let settled_event = TaskEventEntry::new(&task_a, TaskEvent::Succeed);
    Actuator::<DAG>::settle(&mut db, &settled_event)
        .await
        .expect("settle job");
    let settled = Job::filter_by_id(job_a.id).get(&mut db).await.unwrap();
    assert_eq!(settled.status, Status::Succeeded);
    assert!(settled.agent_id.is_none());
    assert!(settled.polled_at.is_none());
    if owner == agent_c {
        assert_eq!(left[0].id, job_a.id);
    } else {
        assert_eq!(right[0].id, job_a.id);
    }

    // No scheduler may reclaim any terminal status or claims made by an
    // external scheduler that does not use agent polling leases.
    let mut protected = Vec::new();
    for status in [
        Status::Succeeded,
        Status::Failed,
        Status::Terminated,
        Status::Polled,
        Status::Pending,
    ] {
        let mut job = Job::new(
            &Task::new(data_id, graph_id, Uuid::new_v4(), 0),
            run.uuid,
            0,
            0,
        );
        job.status = status;
        if status.is_terminal() {
            job.agent_id = Some(Uuid::new_v4());
            job.polled_at = Some(expired_at);
        }
        job.write(&mut db).await.unwrap();
        protected.push((job.uuid, status));
    }
    assert_eq!(AgentScheduler::new().schedule(&mut db).await.unwrap(), 0);
    for (uuid, status) in protected {
        assert_eq!(
            Job::filter_by_uuid(uuid).get(&mut db).await.unwrap().status,
            status
        );
    }

    // The polling endpoint also recovers abandoned claims using its lease.
    Job::filter_by_id(job_b.id)
        .update()
        .status(Status::Pending)
        .polled_at(Some(expired_at))
        .exec(&mut db)
        .await
        .unwrap();
    let recovered = poll_jobs_with_lease(
        &mut db,
        Uuid::new_v4(),
        capacity_a,
        1,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].id, job_b.id);
    assert_ne!(recovered[0].agent_id, Some(agent_a));
}

#[tokio::test]
async fn poll_checks_both_resources_and_scans_past_unfitting_candidates() {
    let Some(mut db) = setup().await else {
        return;
    };
    let task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 0);
    let mut expected = Vec::new();
    // After the first fit, a full page of candidates exceeds the remaining
    // budget. The tiny job after that page must still be considered.
    let requirements = [(5, 0), (0, 5), (3, 3)]
        .into_iter()
        .chain(std::iter::repeat_n((2, 2), 128))
        .chain([(1, 1)]);
    for (cpu, memory) in requirements {
        let mut job = Job::new(&task, Uuid::new_v4(), 0, 0);
        job.required_cpu_cores = cpu;
        job.required_memory_bytes = memory;
        if cpu == 3 || cpu == 1 {
            expected.push(job.uuid);
        }
        job.write(&mut db).await.unwrap();
    }
    let agent = Uuid::new_v4();
    let capacity = ResourceCapacity {
        cpu_cores: 4,
        memory_bytes: 4,
    };
    assert!(
        poll_jobs(&mut db, agent, capacity, 0)
            .await
            .unwrap()
            .is_empty()
    );
    let claimed = poll_jobs(&mut db, agent, capacity, 1000).await.unwrap();
    assert_eq!(
        claimed.iter().map(|job| job.uuid).collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        claimed
            .iter()
            .map(|job| job.required_cpu_cores)
            .sum::<u32>(),
        4
    );
    for job in &claimed {
        Job::filter_by_id(job.id)
            .update()
            .status(Status::Pending)
            .exec(&mut db)
            .await
            .unwrap();
    }
    assert_eq!(
        claimed
            .iter()
            .map(|job| job.required_memory_bytes)
            .sum::<u64>(),
        4
    );
    assert!(
        poll_jobs(&mut db, agent, capacity, 1)
            .await
            .unwrap()
            .is_empty()
    );

    // Zero capacity permits only a job with no resource requirements.
    let free = Job::new(&task, Uuid::new_v4(), 0, 0);
    free.write(&mut db).await.unwrap();
    let claimed = poll_jobs(&mut db, Uuid::new_v4(), ResourceCapacity::default(), 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].uuid, free.uuid);
}
