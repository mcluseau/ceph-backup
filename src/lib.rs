pub mod rbd;

use log::info;
use std::sync::atomic::{AtomicU8, Ordering::Relaxed as Ordering};
use std::sync::Arc;

const PARALLEL: AtomicU8 = AtomicU8::new(1);

pub fn get_parallel() -> u8 {
    PARALLEL.load(Ordering)
}

pub fn set_parallel(v: u8) {
    PARALLEL.store(v, Ordering);
}

pub fn parallel_process<T: Send, R: Send, F>(inputs: Vec<T>, process: F) -> Vec<R>
where
    F: Fn(T) -> R,
    F: Send + Sync,
{
    let parallel = get_parallel();

    if parallel == 1 {
        return inputs.into_iter().map(|input| process(input)).collect();
    }

    let (result_sender, result_receiver) = crossbeam_channel::bounded(0);
    let (sender, receiver) = crossbeam_channel::bounded(inputs.len());

    for (idx, input) in inputs.into_iter().enumerate() {
        sender.send((idx, input)).unwrap();
    }

    let process = Arc::new(process);

    let mut results: Vec<_> = std::thread::scope(|scope| {
        for idx in 0..parallel {
            info!("starting thread {idx}");
            let input_receiver = receiver.clone();
            let result_sender = result_sender.clone();
            let process = process.clone();
            scope.spawn(move || {
                for (idx, input) in input_receiver.into_iter() {
                    let result = process(input);
                    result_sender.send((idx, result)).unwrap();
                }
            });
        }

        result_receiver.iter().collect()
    });

    results.sort_by_key(|v| v.0);
    results.into_iter().map(|(_, r)| r).collect()
}
