use crate::scheduler::workstealing::task::{Task, SchedulerTaskState};
use crate::scheduler::schedproto::{TaskId, TaskInfo, NewFinishedTaskInfo, WorkerId};
use crate::common::{Map, Set};
use crate::scheduler::workstealing::worker::WorkerRef;

#[derive(Default, Debug)]
pub struct TaskMap {
    tasks: Vec<Task>,
    taskid_to_index: Map<TaskId, usize>
}

impl TaskMap {
    #[inline]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item=&Task> {
        self.tasks.iter()
    }

    #[inline]
    pub fn get_task(&self, task_id: TaskId) -> &Task {
        &self.tasks[*self.taskid_to_index
            .get(&task_id)
            .unwrap_or_else(|| panic!("Task {} not found", task_id))]
    }
    #[inline]
    pub fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
        &mut self.tasks[*self.taskid_to_index
            .get(&task_id)
            .unwrap_or_else(|| panic!("Task {} not found", task_id))]
    }

    #[inline(always)]
    pub fn add_task(&mut self, task: Task) {
        assert!(self.taskid_to_index.insert(task.id, self.tasks.len()).is_none());
        self.tasks.push(task);
    }
    #[inline]
    pub fn remove_task(&mut self, task_id: TaskId) {
        let index = self.taskid_to_index[&task_id];
        let last_task_id = self.tasks.last().unwrap().id; // TODO
        self.tasks.swap_remove(index as usize);
        *self.taskid_to_index.get_mut(&last_task_id).unwrap() = index;
        assert!(self.taskid_to_index.remove(&task_id).is_some());
    }

    pub fn new_task(&mut self, ti: TaskInfo) -> &Task {
        let task_id = ti.id;
        let task = {
            let mut unfinished_deps = 0;
            for &dep_id in &ti.inputs {
                let mut t = self.get_task_mut(dep_id);
                t.consumers.push(task_id);
                if t.is_waiting() {
                    unfinished_deps += 1;
                } else {
                    assert!(t.is_finished());
                }
            }
            Task {
                id: ti.id,
                inputs: ti.inputs,
                state: SchedulerTaskState::Waiting,
                b_level: 0.0,
                unfinished_deps,
                size: 0u64,
                consumers: Default::default(),
                assigned_worker: None,
                placement: Default::default(),
                pinned: false,
                take_flag: false,
            }
        };
        self.add_task(task);
        self.get_task(task_id)
    }

    pub fn new_finished_task(&mut self, ti: NewFinishedTaskInfo, placement: Set<WorkerRef>) -> &Task {
        let task = Task {
            id: ti.id,
            inputs: Default::default(),
            state: SchedulerTaskState::Finished,
            b_level: 0.0,
            unfinished_deps: Default::default(),
            size: ti.size,
            consumers: Default::default(),
            assigned_worker: None,
            placement,
            pinned: false,
            take_flag: false,
        };
        self.add_task(task);
        self.get_task(ti.id)
    }

    pub fn sanity_check(&self) -> Vec<WorkerId> {
        let mut workers = vec!();
        /*for (task_id, index) in &self.taskid_to_index {
            let task = &self.tasks[*index];
            assert_eq!(task.id, *task_id);
            task.sanity_check();
            if let Some(w) = &task.assigned_worker {
                workers.push(w.get().id);
            }
        }*/
        workers
    }
}
