use super::worker::WorkerRef;
use crate::common::Set;
use crate::scheduler::schedproto::TaskId;

#[derive(Debug)]
pub enum SchedulerTaskState {
    Waiting,
    Finished,
}

#[derive(Debug)]
pub struct Task {
    pub id: TaskId,
    pub state: SchedulerTaskState,
    pub inputs: Vec<TaskId>,
    pub consumers: Vec<TaskId>,
    pub b_level: f32,
    pub unfinished_deps: u32,
    pub assigned_worker: Option<WorkerRef>,
    pub placement: Set<WorkerRef>,
    pub size: u64,
    pub pinned: bool,
    pub take_flag: bool, // Used in algorithms, no meaning between calls
}

impl Task {
    #[inline]
    pub fn is_waiting(&self) -> bool {
        match self.state {
            SchedulerTaskState::Waiting => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        match self.state {
            SchedulerTaskState::Finished => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_deps == 0
    }

    pub fn sanity_check(&self) {
        /*let mut unfinished = 0;
        for inp in &self.inputs {
            let ti = inp.get();
            if let SchedulerTaskState::Waiting = ti.state {
                unfinished += 1;
            }
            assert!(ti.consumers.contains(task_ref));
        }
        assert_eq!(unfinished, self.unfinished_deps);

        match self.state {
            SchedulerTaskState::Waiting => {
                for c in &self.consumers {
                    assert!(c.get().is_waiting());
                }
            }
            SchedulerTaskState::Finished => {
                for inp in &self.inputs {
                    assert!(inp.get().is_finished());
                }
            }
        };*/
    }
}
