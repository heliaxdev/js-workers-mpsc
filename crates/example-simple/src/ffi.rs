//! `#[wasm_bindgen]` exports loaded by the bridge worker.
//!
//! `dispatch` is generic, so it can't be exported through `wasm-bindgen`
//! directly. Each concrete task needs its own export that fixes the type
//! parameters and hands the channel to the factory. [`run_task`] is one
//! such export; copy it for your own tasks:
//!
//! ```ignore
//! #[wasm_bindgen]
//! pub fn your_task(payload: JsValue, on_result: js_sys::Function) -> js_sys::Promise {
//!     js_workers_mpsc::dispatch(&on_result, async move |sender| {
//!         // compute + send; propagate send errors as Err
//!         Ok(())
//!     })
//! }
//! ```

use js_sys::Promise;
use js_workers_mpsc::{Sender, dispatch};
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

/// A single Fibonacci number, serialized as `{ value: n }`.
#[derive(Serialize)]
pub struct Fibonacci {
    value: u64,
}

/// Why a dispatched task ended in `Err`. Serialized so JS can catch it.
#[derive(Serialize)]
pub enum TaskError {
    /// The JS side is gone: the callback threw or the stream was cancelled.
    ChannelClosed,
}

/// Stream the first `payload` Fibonacci numbers to `on_result`, clamped to 92
/// (fib(94) overflows u64).
///
/// `payload` is read as a number via `JsValue::as_f64` and defaults to 10 for
/// non-numbers. Each value is sent as a [`Fibonacci`]; the `JsFuture` yield
/// between sends lets the drain loop deliver items incrementally rather than
/// batching them at the end.
#[wasm_bindgen]
pub fn run_task(payload: JsValue, on_result: js_sys::Function) -> Promise {
    // Clamped to 92: the loop's final `(a, b) = (b, a + b)` computes
    // fib(n + 1), and fib(94) overflows u64. A panic here would abort the
    // worker before the stream could report an error.
    let n = (payload.as_f64().unwrap_or(10.0) as u32).min(92);

    dispatch(&on_result, async move |sender: Sender<Fibonacci>| {
        let (mut a, mut b) = (0u64, 1u64);

        for _ in 0..n {
            // unbounded_send only fails once the receiver is gone: the
            // callback threw or the stream was cancelled. Propagate so the
            // promise rejects and JS can restart the task.
            sender
                .unbounded_send(Fibonacci { value: a })
                .map_err(|_| TaskError::ChannelClosed)?;

            // Yield between sends so the drain loop can deliver each item
            // as it's produced; drop this to batch everything at the end.
            let _ = JsFuture::from(Promise::resolve(&JsValue::UNDEFINED)).await;

            (a, b) = (b, a + b);
        }

        // `sender` drops here -> channel closes -> promise resolves.
        Ok::<(), TaskError>(())
    })
}
