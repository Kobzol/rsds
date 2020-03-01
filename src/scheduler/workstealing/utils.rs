use crate::common::Map;
use crate::scheduler::schedproto::TaskId;
use crate::scheduler::workstealing::taskmap::TaskMap;

pub fn compute_b_level(tasks: &mut TaskMap) {
    let mut n_consumers: Map<TaskId, u32> = Map::with_capacity(tasks.len());
    let mut stack: Vec<TaskId> = Vec::new();
    for task in tasks.iter() {
        let len = task.consumers.len() as u32;
        if len == 0 {
            //tref.get_mut().b_level = 0.0;
            stack.push(task.id);
        } else {
            n_consumers.insert(task.id, len);
        }
    }
    while let Some(task_id) = stack.pop() {
        let b_level = {
            let task = tasks.get_task(task_id);

            let mut b_level = 0.0f32;
            for &task_id in &task.consumers {
                b_level = b_level.max(tasks.get_task(task_id).b_level);
            }

            for inp in &task.inputs {
                let v: &mut u32 = n_consumers.get_mut(&inp).unwrap();
                if *v <= 1 {
                    assert_eq!(*v, 1);
                    stack.push(inp.clone());
                } else {
                    *v -= 1;
                }
            }

            b_level
        };

        tasks.get_task_mut(task_id).b_level = b_level + 1.0f32;
    }
}
