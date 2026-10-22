//! A pilot can submit work through another scheduler without worker tables.
//! Set DATABASE_URL to an isolated MySQL database; this test resets its schema.

use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use coordinator::mysql::MySqlLeaderElection;
use event::task::Task;
use pilot::{Pilot, Step};
use runtime::backend::Backend;
use scheduler::{Job, JobWrite, Scheduler, actuator::Status};
use server::StaticGraphStore;
use toasty::{Db, Transaction};
use uuid::Uuid;

#[derive(Default)]
struct JobService {
    jobs: Mutex<HashSet<Uuid>>,
    calls: AtomicUsize,
    fail_after_submit: AtomicBool,
}

struct JobScheduler(Arc<JobService>);

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    Database(#[from] toasty::Error),
    #[error("submission response lost")]
    ResponseLost,
}

impl Scheduler<Transaction<'_>> for JobScheduler {
    type Err = Error;

    async fn schedule(&self, backend: &mut Transaction<'_>) -> Result<usize, Error> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        let jobs = Job::all()
            .filter(Job::fields().status().eq(Status::Scheduled))
            .order_by(Job::fields().id().asc())
            .limit(2)
            .exec(&mut *backend)
            .await?;
        let mut submitted = 0;
        for job in jobs {
            // These requests were committed before the pilot pass. Their UUIDs
            // survive rollback and let the remote service deduplicate retries.
            if self.0.jobs.lock().unwrap().insert(job.uuid) {
                submitted += 1;
            }
            Job::filter_by_id(job.id)
                .update()
                .status(Status::Polled)
                .exec(&mut *backend)
                .await?;
        }
        if self.0.fail_after_submit.swap(false, Ordering::SeqCst) {
            return Err(Error::ResponseLost);
        }
        Ok(submitted)
    }
}

#[tokio::test]
async fn custom_scheduler_retries_submissions_and_runs_only_as_leader() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: set DATABASE_URL to run the custom scheduler test");
        return;
    };
    let mut db = Db::builder()
        .models(scheduler::models())
        .connect(&url)
        .await
        .unwrap();
    db.reset_db().await.unwrap();
    db.push_schema().await.unwrap();
    // Commit requests before remote submission: records created in a failed
    // pilot pass would be regenerated with different UUIDs on retry.
    let mut identities = HashSet::new();
    for _ in 0..3 {
        let task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 0);
        let record = Job::new(&task, Uuid::new_v4(), 0, 0);
        identities.insert(record.uuid);
        record.write(&mut db).await.unwrap();
    }
    let service = Arc::new(JobService::default());
    service.fail_after_submit.store(true, Ordering::SeqCst);
    let backend = Backend::new(db.clone(), StaticGraphStore::new());
    let election = MySqlLeaderElection::new(
        db.clone(),
        "custom-scheduler",
        "leader",
        Duration::from_secs(30),
    );
    let mut pilot = Pilot::new(election, backend.clone(), 64, Duration::from_millis(10))
        .with_scheduler(JobScheduler(service.clone()));

    assert!(matches!(
        pilot.step().await,
        Err(pilot::Error::Scheduler(_))
    ));
    assert_eq!(service.jobs.lock().unwrap().len(), 2);
    assert!(
        Job::all()
            .exec(&mut db)
            .await
            .unwrap()
            .iter()
            .all(|job| job.status == Status::Scheduled)
    );

    // Retrying reconciles already submitted jobs and commits their state.
    pilot.step().await.unwrap();
    assert_eq!(service.jobs.lock().unwrap().len(), 2);
    assert_eq!(
        Job::all()
            .filter(Job::fields().status().eq(Status::Polled))
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        2
    );
    let election = MySqlLeaderElection::new(
        db.clone(),
        "custom-scheduler",
        "standby",
        Duration::from_secs(30),
    );
    let mut standby = Pilot::new(election, backend, 64, Duration::from_millis(10))
        .with_scheduler(JobScheduler(service.clone()));
    assert_eq!(standby.step().await.unwrap(), Step::default());
    assert_eq!(service.calls.load(Ordering::SeqCst), 2);

    pilot.step().await.unwrap();
    assert_eq!(*service.jobs.lock().unwrap(), identities);
    assert!(
        Job::all()
            .exec(&mut db)
            .await
            .unwrap()
            .iter()
            .all(|job| job.status == Status::Polled)
    );
}
