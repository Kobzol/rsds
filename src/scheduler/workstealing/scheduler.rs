use super::task::{SchedulerTaskState, Task};
use super::utils::compute_b_level;
use super::worker::{Worker, WorkerRef, HostnameId};
use crate::common::{Map, Set};
use crate::scheduler::comm::SchedulerComm;
use crate::scheduler::schedproto::{
    SchedulerRegistration, TaskAssignment, TaskId, TaskStealResponse, TaskUpdate, TaskUpdateType,
    WorkerId,
};
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use futures::StreamExt;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::time::Duration;
use crate::scheduler::workstealing::taskmap::TaskMap;

#[derive(Debug)]
pub struct Scheduler {
    network_bandwidth: f32,
    workers: Map<WorkerId, WorkerRef>,
    tasks: TaskMap,
    ready_to_assign: Vec<TaskId>,
    new_tasks: Vec<TaskId>,
    hostnames: Map<String, HostnameId>,
    rng: ThreadRng,
}

type Notifications = Set<TaskId>;

const MIN_SCHEDULING_DELAY: Duration = Duration::from_millis(15);

impl Scheduler {
    pub fn new() -> Self {
        Self {
            workers: Default::default(),
            tasks: Default::default(),
            ready_to_assign: Default::default(),
            new_tasks: Default::default(),
            hostnames: Default::default(),
            network_bandwidth: 100.0, // Guess better default
            rng: thread_rng(),
        }
    }

    fn get_worker(workers: &Map<WorkerId, WorkerRef>, worker_id: WorkerId) -> &WorkerRef {
        workers
            .get(&worker_id)
            .unwrap_or_else(|| panic!("Worker {} not found", worker_id))
    }

    fn send_notifications(&self, notifications: Notifications, sender: &mut SchedulerComm) {
        let assignments: Vec<_> = notifications
            .into_iter()
            .map(|tr| {
                let task = self.tasks.get_task(tr);
                let worker_ref = task.assigned_worker.clone().unwrap();
                let worker = worker_ref.get();
                TaskAssignment {
                    task: task.id,
                    worker: worker.id,
                    priority: 0,
                    // TODO derive priority from b-level
                }
            })
            .collect();
        sender.send(FromSchedulerMessage::TaskAssignments(assignments));
    }

    pub async fn start(mut self, mut comm: SchedulerComm) -> crate::Result<()> {
        log::debug!("Workstealing scheduler initialized");

        comm.send(FromSchedulerMessage::Register(SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "workstealing-scheduler".into(),
            scheduler_version: "0.0".into(),
        }));

        while let Some(msgs) = comm.recv.next().await {
            /* TODO: Add delay that prevents calling scheduler too often */
            if self.update(msgs) {
                let mut notifications = Notifications::new();

                trace_time!("schedule", {
                    if self.schedule(&mut notifications) {
                        trace_time!("balance", self.balance(&mut notifications));
                    }
                });

                self.send_notifications(notifications, &mut comm);
            }
        }
        log::debug!("Scheduler closed");
        Ok(())
    }

    /// Returns true if balancing is needed.
    pub fn schedule(&mut self, mut notifications: &mut Notifications) -> bool {
        if self.workers.is_empty() {
            return false;
        }

        log::debug!("Scheduling started");
        if !self.new_tasks.is_empty() {
            // TODO: utilize information and do not recompute all b-levels
            compute_b_level(&mut self.tasks);
            self.new_tasks = Vec::new()
        }

        for tr in std::mem::take(&mut self.ready_to_assign).into_iter() {
            let worker_ref = Scheduler::choose_worker_for_task(&self.workers, &self.tasks, &self.tasks.get_task(tr));
            let mut task = self.tasks.get_task_mut(tr);
            let mut worker = worker_ref.get_mut();
            log::debug!("Task {} initially assigned to {}", task.id, worker.id);
            assert!(task.assigned_worker.is_none());
            assign_task_to_worker(
                task,
                &mut worker,
                worker_ref.clone(),
                &mut notifications,
            );
        }

        let has_underload_workers = self.workers.values().any(|wr| {
            let worker = wr.get();
            worker.is_underloaded()
        });

        log::debug!("Scheduling finished");
        has_underload_workers
    }

    fn balance(&mut self, mut notifications: &mut Notifications) {
        log::debug!("Balancing started");
        let mut balanced_tasks = Vec::new();
        for wr in self.workers.values() {
            let worker = wr.get();
            let len = worker.tasks.len() as u32;
            if len > worker.ncpus {
                log::debug!("Worker {} offers {} tasks", worker.id, len);
                for &tr in &worker.tasks {
                    self.tasks.get_task_mut(tr).take_flag = false;
                }
                balanced_tasks.extend(worker.tasks.iter().filter(|tr| !self.tasks.get_task(**tr).pinned).copied());
            }
        }

        let mut underload_workers = Vec::new();
        for wr in self.workers.values() {
            let worker = wr.get();
            let len = worker.tasks.len() as u32;
            if len < worker.ncpus {
                log::debug!("Worker {} is underloaded ({} tasks)", worker.id, len);
                let mut ts = balanced_tasks.clone();
                ts.sort_by_cached_key(|&tr| std::u64::MAX - task_transfer_cost(&self.tasks, self.tasks.get_task(tr), &wr));
                underload_workers.push((wr.clone(), ts));
            }
        }
        underload_workers.sort_by_key(|x| x.0.get().tasks.len());

        let mut n_tasks = underload_workers[0].0.get().tasks.len();
        loop {
            let mut change = false;
            for (wr, ts) in underload_workers.iter_mut() {
                let mut worker = wr.get_mut();
                if worker.tasks.len() > n_tasks {
                    break;
                }
                if ts.is_empty() {
                    continue;
                }
                while let Some(tr) = ts.pop() {
                    let mut task = self.tasks.get_task_mut(tr);
                    if task.take_flag {
                        continue;
                    }
                    task.take_flag = true;
                    let wid = {
                        let wr2 = task.assigned_worker.clone().unwrap();
                        let worker2 = wr2.get();
                        if worker2.tasks.len() <= n_tasks {
                            continue;
                        }
                        worker2.id
                    };
                    log::debug!(
                        "Changing assignment of task={} from worker={} to worker={}",
                        task.id,
                        wid,
                        worker.id
                    );
                    assign_task_to_worker(
                        &mut task,
                        &mut worker,
                        wr.clone(),
                        &mut notifications,
                    );
                    break;
                }
                change = true;
            }
            if !change {
                break;
            }
            n_tasks += 1;
        }
        log::debug!("Balancing finished");
    }

    fn task_update(&mut self, tu: TaskUpdate) -> bool {
        match tu.state {
            TaskUpdateType::Finished => {
                let mut invoke_scheduling = {
                    let task = self.tasks.get_task_mut(tu.id);

                    log::debug!("Task id={} is finished on worker={}", task.id, tu.worker);

                    let worker = Scheduler::get_worker(&self.workers, tu.worker).clone();
                    assert!(task.is_waiting() && task.is_ready());
                    task.state = SchedulerTaskState::Finished;
                    task.size = tu.size.unwrap();
                    let wr = task.assigned_worker.take().unwrap();
                    let mut invoke_scheduling = {
                        let mut worker = wr.get_mut();
                        assert!(worker.tasks.remove(&task.id));
                        worker.is_underloaded()
                    };
                    task.placement.insert(worker);
                    invoke_scheduling
                };

                // Borrow checker error
                for &tref in &self.tasks.get_task(tu.id).consumers {
                    let mut t = self.tasks.get_task_mut(tref);
                    if t.unfinished_deps <= 1 {
                        assert!(t.unfinished_deps > 0);
                        assert!(t.is_waiting());
                        t.unfinished_deps -= 1;
                        log::debug!("Task {} is ready", t.id);
                        self.ready_to_assign.push(tref);
                        invoke_scheduling = true;
                    } else {
                        t.unfinished_deps -= 1;
                    }
                }
                return invoke_scheduling;
            }
            TaskUpdateType::Placed => {
                let task = self.tasks.get_task_mut(tu.id);
                let worker = Scheduler::get_worker(&self.workers, tu.worker).clone();
                assert!(task.is_finished());
                task.placement.insert(worker);
            }
            TaskUpdateType::Removed => {
                let task = self.tasks.get_task_mut(tu.id);
                let worker = Scheduler::get_worker(&self.workers, tu.worker);
                //task.placement.remove(worker);
                if !task.placement.remove(worker) {
                    panic!(
                        "Worker {} removes task {}, but it was not there.",
                        worker.get().id,
                        task.id
                    );
                }
                /*let index = task.placement.iter().position(|x| x == worker).unwrap();
                task.placement.remove(index);*/
            }
        }
        false
    }

    fn rollback_steal(&mut self, response: TaskStealResponse) -> bool {
        let mut task = self.tasks.get_task_mut(response.id);
        let new_wref = Scheduler::get_worker(&self.workers, response.to_worker);

        let need_balancing = {
            let wref = task.assigned_worker.as_ref().unwrap();
            if wref == new_wref {
                return false;
            }
            let mut worker = wref.get_mut();
            worker.tasks.remove(&task.id);
            worker.is_underloaded()
        };
        task.pinned = true;
        task.assigned_worker = Some(new_wref.clone());
        let mut new_worker = new_wref.get_mut();
        new_worker.tasks.insert(task.id);
        need_balancing
    }

    pub fn update(&mut self, messages: Vec<ToSchedulerMessage>) -> bool {
        let mut invoke_scheduling = false;
        for message in messages {
            match message {
                ToSchedulerMessage::TaskUpdate(tu) => {
                    invoke_scheduling |= self.task_update(tu);
                }
                ToSchedulerMessage::TaskStealResponse(sr) => {
                    if !sr.success {
                        invoke_scheduling |= self.rollback_steal(sr);
                    }
                }
                ToSchedulerMessage::NewTask(ti) => {
                    log::debug!("New task {} #inputs={}", ti.id, ti.inputs.len());
                    let task_id = ti.id;
                    let task = self.tasks.new_task(ti);
                    if task.is_ready() {
                        log::debug!("Task {} is ready", task_id);
                        self.ready_to_assign.push(task.id);
                    }
                    self.new_tasks.push(task.id);
                    invoke_scheduling = true;
                }
                ToSchedulerMessage::RemoveTask(task_id) => {
                    log::debug!("Remove task {}", task_id);
                    {
                        let task = self.tasks.get_task(task_id);
                        assert!(task.is_finished()); // TODO: Define semantics of removing non-finished tasks
                    }
                    self.tasks.remove_task(task_id);
                }
                ToSchedulerMessage::NewFinishedTask(ti) => {
                    let placement: Set<WorkerRef> = ti
                        .workers
                        .iter()
                        .map(|id| Scheduler::get_worker(&self.workers, *id).clone())
                        .collect();
                    self.tasks.new_finished_task(ti, placement);
                }
                ToSchedulerMessage::NewWorker(wi) => {
                    let hostname_id = self.get_hostname_id(&wi.hostname);
                    assert!(self.workers.insert(wi.id, WorkerRef::new(wi, hostname_id),).is_none());
                    invoke_scheduling = true;
                }
                ToSchedulerMessage::NetworkBandwidth(nb) => {
                    self.network_bandwidth = nb;
                }
            }
        }
        invoke_scheduling
    }

    pub fn get_hostname_id(&mut self, hostname: &str) -> HostnameId {
        let new_id = self.hostnames.len() as HostnameId;
        *self.hostnames.entry(hostname.to_owned()).or_insert(new_id)
    }

    fn choose_worker_for_task(worker_map: &Map<WorkerId, WorkerRef>, tasks: &TaskMap, task: &Task) -> WorkerRef {
        let mut rng = thread_rng();
        let mut costs = std::u64::MAX;
        let mut workers = Vec::new();
        for wr in worker_map.values() {
            let c = task_transfer_cost(&tasks, task, wr);
            if c < costs {
                costs = c;
                workers.clear();
                workers.push(wr.clone());
            } else if c == costs {
                workers.push(wr.clone());
            }
        }
        if workers.len() == 1 {
            workers.pop().unwrap()
        } else {
            workers.choose(&mut rng).unwrap().clone()
        }
    }

    pub fn sanity_check(&self) {
        /*for (id, tr) in &self.tasks {
            let task = tr.get();
            assert_eq!(task.id, *id);
            task.sanity_check(&tr);
            if let Some(w) = &task.assigned_worker {
                assert!(self.workers.contains_key(&w.get().id));
            }
        }

        for wr in self.workers.values() {
            let worker = wr.get();
            worker.sanity_check(&wr);
        } TODO*/
    }
}

fn assign_task_to_worker(
    task: &mut Task,
    worker: &mut Worker,
    worker_ref: WorkerRef,
    notifications: &mut Notifications,
) {
    notifications.insert(task.id);
    if let Some(wr) = &task.assigned_worker {
        assert!(!wr.eq(&worker_ref));
        let mut previous_worker = wr.get_mut();
        assert!(previous_worker.tasks.remove(&task.id));
    }
    task.assigned_worker = Some(worker_ref);
    assert!(worker.tasks.insert(task.id));
}

fn task_transfer_cost(tasks: &TaskMap, task: &Task, worker_ref: &WorkerRef) -> u64 {
    // TODO: For large number of inputs, only sample inputs
    task.inputs
        .iter()
        .take(512)
        .map(|&tr| {
            let t = tasks.get_task(tr);
            if t.placement.contains(worker_ref) {
                0u64
            } else {
                t.size
            }
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::schedproto::{TaskInfo, WorkerInfo};

    fn init() {
        let _ = env_logger::try_init().map_err(|_| {
            println!("Logging initialized failed");
        });
    }

    fn new_task(id: TaskId, inputs: Vec<TaskId>) -> ToSchedulerMessage {
        ToSchedulerMessage::NewTask(TaskInfo { id, inputs })
    }

    /* Graph simple
         T1
        /  \
       T2   T3
       |  / |\
       T4   | T6
        \      \
         \ /   T7
          T5

    */
    fn submit_graph_simple(scheduler: &mut Scheduler) {
        scheduler.update(vec![
            new_task(1, vec![]),
            new_task(2, vec![1]),
            new_task(3, vec![1]),
            new_task(4, vec![2, 3]),
            new_task(5, vec![4]),
            new_task(6, vec![3]),
            new_task(7, vec![6]),
        ]);
    }

    /* Graph reduce

        0   1   2   3   4 .. n-1
         \  \   |   /  /    /
          \--\--|--/--/----/
                |
                n
    */
    fn submit_graph_reduce(scheduler: &mut Scheduler, size: usize) {
        let mut tasks: Vec<_> = (0..size)
            .map(|t| new_task(t as TaskId, Vec::new()))
            .collect();
        tasks.push(new_task(size as TaskId, (0..size as TaskId).collect()));
        scheduler.update(tasks);
    }

    /* Graph split

        0
        |\---\---\------\
        | \   \   \     \
        1  2   3   4  .. n-1
    */
    fn submit_graph_split(scheduler: &mut Scheduler, size: usize) {
        let mut tasks = vec![new_task(0, Vec::new())];
        tasks.extend((1..=size).map(|t| new_task(t as TaskId, vec![0])));
        scheduler.update(tasks);
    }

    fn connect_workers(scheduler: &mut Scheduler, count: u32, n_cpus: u32) {
        for i in 0..count {
            scheduler.update(vec![ToSchedulerMessage::NewWorker(WorkerInfo {
                id: 100 + i as WorkerId,
                n_cpus,
                hostname: "worker".into()
            })]);
        }
    }

    fn finish_task(scheduler: &mut Scheduler, task_id: TaskId, worker_id: WorkerId, size: u64) {
        scheduler.update(vec![ToSchedulerMessage::TaskUpdate(TaskUpdate {
            state: TaskUpdateType::Finished,
            id: task_id,
            worker: worker_id,
            size: Some(size),
        })]);
    }

    fn run_schedule(scheduler: &mut Scheduler) -> Set<TaskId> {
        let mut notifications = Notifications::new();
        if scheduler.schedule(&mut notifications) {
            scheduler.balance(&mut notifications);
        }
        notifications.iter().map(|tr| tr.get().id).collect()
    }

    fn assigned_worker(scheduler: &mut Scheduler, task_id: TaskId) -> WorkerId {
        scheduler
            .tasks
            .get(&task_id)
            .expect("Unknown task")
            .get()
            .assigned_worker
            .as_ref()
            .expect("Worker not assigned")
            .get()
            .id
    }

    #[test]
    fn test_b_level() {
        let mut scheduler = Scheduler::new();
        submit_graph_simple(&mut scheduler);
        assert_eq!(scheduler.ready_to_assign.len(), 1);
        assert_eq!(scheduler.ready_to_assign[0].get().id, 1);
        connect_workers(&mut scheduler, 1, 1);
        run_schedule(&mut scheduler);
        assert_eq!(scheduler.get_task(7).get().b_level, 1.0);
        assert_eq!(scheduler.get_task(6).get().b_level, 2.0);
        assert_eq!(scheduler.get_task(5).get().b_level, 1.0);
        assert_eq!(scheduler.get_task(4).get().b_level, 2.0);
        assert_eq!(scheduler.get_task(3).get().b_level, 3.0);
        assert_eq!(scheduler.get_task(2).get().b_level, 3.0);
        assert_eq!(scheduler.get_task(1).get().b_level, 4.0);
    }

    #[test]
    fn test_simple_w1_1() {
        let mut scheduler = Scheduler::new();
        submit_graph_simple(&mut scheduler);
        connect_workers(&mut scheduler, 1, 1);
        scheduler.sanity_check();

        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&1));
        scheduler.sanity_check();

        let w = assigned_worker(&mut scheduler, 1);
        finish_task(&mut scheduler, 1, w, 1);
        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 2);
        assert!(n.contains(&2));
        assert!(n.contains(&3));
        let _t2 = scheduler.tasks.get(&2).unwrap();
        let _t3 = scheduler.tasks.get(&3).unwrap();
        assert_eq!(assigned_worker(&mut scheduler, 2), 100);
        assert_eq!(assigned_worker(&mut scheduler, 3), 100);
        scheduler.sanity_check();
    }

    #[test]
    fn test_simple_w2_1() {
        init();
        let mut scheduler = Scheduler::new();
        submit_graph_simple(&mut scheduler);
        connect_workers(&mut scheduler, 2, 1);
        scheduler.sanity_check();

        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&1));
        scheduler.sanity_check();

        let w = assigned_worker(&mut scheduler, 1);
        finish_task(&mut scheduler, 1, w, 1);
        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 2);
        assert!(n.contains(&2));
        assert!(n.contains(&3));
        let _t2 = scheduler.tasks.get(&2).unwrap();
        let _t3 = scheduler.tasks.get(&3).unwrap();
        assert_ne!(
            assigned_worker(&mut scheduler, 2),
            assigned_worker(&mut scheduler, 3)
        );
        scheduler.sanity_check();
    }

    #[test]
    fn test_reduce_w5_1() {
        init();
        let mut scheduler = Scheduler::new();
        submit_graph_reduce(&mut scheduler, 5000);
        connect_workers(&mut scheduler, 5, 1);
        scheduler.sanity_check();

        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 5000);
        scheduler.sanity_check();

        let mut wcount = Map::default();
        for t in 0..5000 {
            assert!(n.contains(&t));
            let wid = assigned_worker(&mut scheduler, t);
            let c = wcount.entry(wid).or_insert(0);
            *c += 1;
        }

        dbg!(&wcount);
        assert_eq!(wcount.len(), 5);
        for v in wcount.values() {
            assert!(*v > 400);
        }
    }

    #[test]
    fn test_split_w5_1() {
        init();
        let mut scheduler = Scheduler::new();
        submit_graph_split(&mut scheduler, 5000);
        connect_workers(&mut scheduler, 5, 1);
        scheduler.sanity_check();

        let _n = run_schedule(&mut scheduler);
        let w = assigned_worker(&mut scheduler, 0);
        finish_task(&mut scheduler, 0, w, 100_000_000);
        scheduler.sanity_check();

        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 5000);
        scheduler.sanity_check();

        let mut wcount = Map::default();
        for t in 1..=5000 {
            assert!(n.contains(&t));
            let wid = assigned_worker(&mut scheduler, t);
            let c = wcount.entry(wid).or_insert(0);
            *c += 1;
        }

        dbg!(&wcount);
        assert_eq!(wcount.len(), 5);
        for v in wcount.values() {
            assert!(*v > 400);
        }
    }

    #[test]
    fn test_consecutive_submit() {
        init();
        let mut scheduler = Scheduler::new();
        connect_workers(&mut scheduler, 5, 1);
        scheduler.update(vec![new_task(1, vec![])]);
        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&1));
        scheduler.sanity_check();
        finish_task(&mut scheduler, 1, 101, 1000_1000);
        scheduler.sanity_check();
        scheduler.update(vec![new_task(2, vec![1])]);
        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&2));
        scheduler.sanity_check();
    }
}
