use crate::{LocalWorker, Worker};
use rivet_core::{RivetError, Task, TaskResult, WorkerId};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

#[derive(Debug)]
pub struct WorkerHandle {
    pub id: WorkerId,
    inbox: Option<mpsc::Sender<Task>>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl WorkerHandle {
    pub fn get_id(&self) -> WorkerId {
        self.id
    }

    pub fn send(&self, task: Task) -> Result<(), RivetError> {
        match self.inbox.as_ref() {
            Some(result) => {
                let res = result.send(task);
                match res {
                    Ok(()) => {
                        Ok(())
                    }
                    Err(res) => {
                        Err(RivetError::Other(res.to_string()))
                    }
                }
            }
            None => {
                Err(RivetError::Other(
                    String::from("Could not call send()") ,
                ))
            }
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.inbox.take();
        for t in self.threads.drain(std::ops::RangeFull) {
            let _ = t.join();
        }
    }
}

pub fn spawn(capacity: usize, result_sender: mpsc::Sender<TaskResult>) -> WorkerHandle {
    let (task_sender, task_receiver) = mpsc::channel::<Task>();
    let receiver_mutex_ref = Arc::new(Mutex::new(task_receiver));
    let local_worker = LocalWorker::new();
    let worker_id = *local_worker.get_id();
    let worker = Arc::new(local_worker);
    let mut threads = Vec::<thread::JoinHandle<()>>::with_capacity(capacity);

    for _i in 0..capacity {
        let receiver_mutex = Arc::clone(&receiver_mutex_ref);
        let worker = Arc::clone(&worker);
        let sender = result_sender.clone();

        threads.push(std::thread::spawn(move || loop {
            let received = {
                let guard = receiver_mutex.lock().unwrap();
                guard.recv()
            };

            match received {
                Ok(task) => {
                    let task_id = task.id;
                    let result = worker.execute(task);
                    match result {
                        Ok(result) => {
                            let _ = sender.send(result);
                        }
                        Err(err) => {
                            let _ = sender.send(TaskResult::Failure {
                                task_id,
                                error: err.to_string(),
                            });
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }));
    }
    WorkerHandle {
        id: worker_id,
        inbox: Some(task_sender),
        threads,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::TaskPayload;
    use std::time::{Duration, Instant};

    /// Every test uses `recv_timeout` rather than `recv` so a bug fails the
    /// test instead of hanging the whole suite.
    const TIMEOUT: Duration = Duration::from_secs(5);

    fn task(name: &str) -> Task {
        Task::new(TaskPayload::new(name))
    }

    #[test]
    fn spawned_worker_executes_a_sent_task() {
        let (tx, rx) = mpsc::channel();
        let handle = spawn(1, tx);

        let t = task("noop");
        let id = t.id;
        handle.send(t).expect("send should succeed");

        let result = rx.recv_timeout(TIMEOUT).expect("a result should arrive");
        assert_eq!(result.task_id(), id, "result must reference the sent task");
        assert!(result.is_success());
    }

    #[test]
    fn spawned_worker_handles_several_tasks_in_sequence() {
        let (tx, rx) = mpsc::channel();
        let handle = spawn(1, tx);

        let mut sent = Vec::new();
        for i in 0..3 {
            let t = task(&format!("job-{i}"));
            sent.push(t.id);
            handle.send(t).expect("send should succeed");
        }

        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(
                rx.recv_timeout(TIMEOUT)
                    .expect("a result should arrive")
                    .task_id(),
            );
        }

        sent.sort_by_key(|id| id.as_u64());
        seen.sort_by_key(|id| id.as_u64());
        assert_eq!(seen, sent, "every task sent should come back exactly once");
    }

    #[test]
    fn capacity_two_runs_two_tasks_concurrently() {
        let (tx, rx) = mpsc::channel();
        let handle = spawn(2, tx);

        let start = Instant::now();
        handle.send(task("a")).unwrap();
        handle.send(task("b")).unwrap();
        for _ in 0..2 {
            rx.recv_timeout(TIMEOUT).expect("a result should arrive");
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(180),
            "two 100ms tasks on capacity 2 should overlap, but took {elapsed:?}. \
             If this is close to 200ms, the receiver lock is being held across execute()."
        );
    }

    #[test]
    fn dropping_the_handle_waits_for_in_flight_work() {
        let (tx, rx) = mpsc::channel();
        let handle = spawn(1, tx);
        handle.send(task("slow")).unwrap();

        // Let the worker actually pick the task up, so we measure the join
        // rather than racing the dispatch.
        thread::sleep(Duration::from_millis(20));

        let start = Instant::now();
        drop(handle);
        let elapsed = start.elapsed();

        // A Drop impl that joins must wait for the ~100ms task to finish.
        // Without one the threads are detached and drop returns immediately.
        assert!(
            elapsed >= Duration::from_millis(50),
            "drop returned in {elapsed:?}, so it did not join the worker threads. \
             WorkerHandle needs a Drop impl: take the inbox, then join."
        );
        assert!(
            rx.try_recv().is_ok(),
            "after a joining drop, the in-flight result should already be sent"
        );
    }

    #[test]
    fn dropping_the_handle_closes_the_results_channel() {
        let (tx, rx) = mpsc::channel();
        let handle = spawn(2, tx.clone());

        // The test must not keep a results sender alive, or the channel below
        // can never close.
        drop(tx);
        // Dropping the handle drops the inbox sender, so every worker thread's
        // recv() returns Err and the thread exits, dropping its results sender.
        drop(handle);

        match rx.recv_timeout(TIMEOUT) {
            Err(mpsc::RecvTimeoutError::Disconnected) => {}
            other => panic!("expected the results channel to close, got {other:?}"),
        }
    }
}
