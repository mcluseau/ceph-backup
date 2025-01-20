pub mod rbd;

use log::info;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering::Relaxed as Ordering};
use std::time::Duration;

const PARALLEL: AtomicU8 = AtomicU8::new(1);

pub fn get_parallel() -> u8 {
    PARALLEL.load(Ordering)
}

pub fn set_parallel(v: u8) {
    PARALLEL.store(v, Ordering);
}

const SIGTERM: AtomicBool = AtomicBool::new(false);

pub fn sigterm() {
    SIGTERM.store(true, Ordering);
}
pub fn terminated() -> bool {
    SIGTERM.load(Ordering)
}

pub fn parallel_process<T: Send, R: Send, F>(
    inputs: impl IntoIterator<Item = T> + Send,
    process: F,
) -> Vec<R>
where
    F: Fn(T) -> R,
    F: Send + Sync,
{
    let parallel = get_parallel();

    let (result_tx, result_rx) = crossbeam_channel::bounded(0);
    let (input_tx, input_rx) = crossbeam_channel::bounded(0);

    let mut results: Vec<_> = std::thread::scope(|scope| {
        scope.spawn(move || {
            for item in inputs.into_iter().enumerate() {
                let mut item = item;
                loop {
                    if terminated() {
                        return;
                    }
                    use crossbeam_channel::SendTimeoutError::*;
                    item = match input_tx.send_timeout(item, Duration::from_secs(1)) {
                        Ok(_) => {
                            break;
                        }
                        Err(Disconnected(_)) => {
                            return;
                        }
                        Err(Timeout(sent_item)) => sent_item,
                    };
                }
            }
        });

        for idx in 0..parallel {
            if parallel > 1 {
                info!("starting thread {idx}");
            }
            let input_rx = input_rx.clone();
            let result_tx = result_tx.clone();
            let process = &process;
            scope.spawn(move || {
                for (idx, input) in input_rx.into_iter() {
                    let result = process(input);
                    result_tx.send((idx, result)).unwrap();
                }
            });
        }

        drop(result_tx);
        result_rx.iter().collect()
    });

    if terminated() {
        std::process::exit(1);
    }

    results.sort_by_key(|v| v.0);
    results.into_iter().map(|(_, r)| r).collect()
}
