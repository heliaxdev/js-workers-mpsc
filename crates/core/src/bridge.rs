//! Implementation of [`stream_channel`].

use futures::StreamExt;
use futures::channel::mpsc;
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

// Re-exported so downstream code doesn't need a direct `futures` dependency.
pub use futures::channel::mpsc::{TrySendError, UnboundedSender as Sender};

/// Create an mpsc channel that streams each item to `on_result` and returns
/// a [`Promise`][js_sys::Promise] that settles when the stream ends.
///
/// The returned [`Sender`] is unbounded and non-blocking: call
/// [`Sender::unbounded_send`] from your compute as results arrive. The
/// receiver is drained on the worker's thread via `wasm-bindgen-futures`, so
/// the JS event loop is never blocked and items are delivered incrementally.
///
/// The promise resolves when all clones of the sender have been dropped. If
/// `on_result` throws or an item fails to serialize, the promise rejects and
/// subsequent sends return [`TrySendError`].
///
/// `T` is serialized with `serde-wasm-bindgen`; prefer flat, fixed-size types
/// to avoid allocator churn during long-running streams.
pub fn stream_channel<T: Serialize + Send + 'static>(
    on_result: &js_sys::Function,
) -> (Sender<T>, js_sys::Promise) {
    let (tx, mut rx) = mpsc::unbounded::<T>();
    let on_result = on_result.clone();

    let promise = future_to_promise(async move {
        while let Some(item) = rx.next().await {
            let js_item = serde_wasm_bindgen::to_value(&item)
                .map_err(|err| JsValue::from_str(&err.to_string()))?;
            on_result.call1(&JsValue::NULL, &js_item)?;
        }
        Ok(JsValue::UNDEFINED)
    });

    (tx, promise)
}
