//! `#[wasm_bindgen]` exports loaded by the bridge worker.
//!
//! The shape matches the simple example. The difference is that the channel
//! is fed from inside a [`rayon::scope`]: each pooled task computes one item
//! and pushes it through its cloned [`Sender`], so results arrive in pool
//! completion order rather than index order.

use js_sys::Promise;
use js_workers_mpsc::{Sender, stream_channel};
use rayon::scope;
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

/// One streamed result: the input index and its Fibonacci number,
/// serialized as `{ input: i, value: fib(i) }`.
#[derive(Serialize)]
pub struct Fibonacci {
    input: u32,
    value: u64,
}

fn fib(n: u32) -> u64 {
    let (mut a, mut b) = (0u64, 1u64);
    for _ in 0..n {
        (a, b) = (b, a + b);
    }
    a
}

/// Stream the first `payload` Fibonacci numbers, computed across the Rayon
/// pool.
///
/// `payload` is read as a number via `JsValue::as_f64` and defaults to 10 for
/// non-numbers. Each index is computed by its own pool task; results stream
/// back through `on_result` in completion order. No `JsFuture` yield is
/// needed here: the `unbounded_send`s come from pool threads, so the drain
/// loop wakes up on its own.
#[wasm_bindgen]
pub fn run_parallel_task(payload: JsValue, on_result: js_sys::Function) -> Promise {
    let n = payload.as_f64().unwrap_or(10.0) as u32;

    let (sender, promise) = stream_channel::<Fibonacci>(&on_result);

    spawn_local(async move {
        scope(|s| {
            for i in 0..n {
                let sender = Sender::clone(&sender);
                s.spawn(move |_| {
                    let _ = sender.unbounded_send(Fibonacci {
                        input: i,
                        value: fib(i),
                    });
                });
            }
            // `scope` joins: all tasks have run and their senders dropped.
        });
        // Last sender drops here -> channel closes -> `promise` resolves.
    });

    promise
}
