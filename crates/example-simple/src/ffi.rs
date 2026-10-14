//! `#[wasm_bindgen]` exports loaded by the bridge worker.
//!
//! `stream_channel` is generic, so it can't be exported through `wasm-bindgen`
//! directly. Each concrete task needs its own export that fixes the type
//! parameter and drives the channel. [`run_task`] is one such export; copy it
//! for your own tasks:
//!
//! ```ignore
//! #[wasm_bindgen]
//! pub fn your_task(payload: JsValue, on_result: js_sys::Function) -> js_sys::Promise {
//!     let (sender, promise) = js_workers_mpsc::stream_channel::<YourItem>(&on_result);
//!     wasm_bindgen_futures::spawn_local(async move { /* compute + send */ });
//!     promise
//! }
//! ```

use js_sys::Promise;
use js_workers_mpsc::stream_channel;
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{spawn_local, JsFuture};

/// A single Fibonacci number, serialized as `{ value: n }`.
#[derive(Serialize)]
pub struct Fibonacci {
    value: u64,
}

/// Stream the first `payload` Fibonacci numbers to `on_result`.
///
/// `payload` is read as a number via `JsValue::as_f64` and defaults to 10 for
/// non-numbers. Each value is sent as a [`Fibonacci`]; the `JsFuture` yield
/// between sends lets the drain loop deliver items incrementally rather than
/// batching them at the end.
#[wasm_bindgen]
pub fn run_task(payload: JsValue, on_result: js_sys::Function) -> Promise {
    let (sender, promise) = stream_channel::<Fibonacci>(&on_result);

    spawn_local(async move {
        let n = payload.as_f64().unwrap_or(10.0) as u32;
        let (mut a, mut b) = (0u64, 1u64);

        for _ in 0..n {
            // unbounded_send only fails if the receiver was dropped (the
            // callback threw).
            let _ = sender.unbounded_send(Fibonacci { value: a });

            // Yield between sends so the drain loop can deliver each item as
            // it's produced; drop this to batch everything at the end.
            let _ = JsFuture::from(Promise::resolve(&JsValue::UNDEFINED)).await;

            (a, b) = (b, a + b);
        }

        // `sender` drops here -> channel closes -> `promise` resolves.
    });

    promise
}
