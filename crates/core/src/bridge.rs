//! Implementation of [`dispatch`].

use futures::StreamExt;
use futures::channel::mpsc;
use futures::future::join;
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

pub use futures::channel::mpsc::{SendError, UnboundedSender as Sender};

/// Dispatch work that streams items to `on_event` and returns a `Result`.
///
/// `factory` receives the sending end of an unbounded mpsc channel and
/// returns `Result<Out, E>` once its compute is done. Each item sent is
/// serialized with `serde-wasm-bindgen` and passed to `on_event`. The
/// factory's `Result` becomes the promise's outcome: `Ok` resolves
/// (serialized), `Err` rejects (serialized).
///
/// The drain loop and the factory run concurrently on the worker's thread
/// via `wasm-bindgen-futures`, so the JS event loop is never blocked. When
/// `on_event` throws (or an item fails to serialize), the receiver drops
/// and the channel closes. The factory learns this through `send`, whose
/// subsequent calls error out. The factory should propagate that as its
/// `Err`. Panics in the factory abort the worker, so panicking under
/// the `factory` handler should be avoided.
pub fn dispatch<T, Out, E>(
    on_event: &js_sys::Function,
    factory: impl AsyncFnOnce(Sender<T>) -> Result<Out, E> + 'static,
) -> js_sys::Promise
where
    T: Serialize + Send + 'static,
    Out: Serialize,
    E: Serialize,
{
    let on_event = on_event.clone();

    future_to_promise(async move {
        let (tx, mut rx) = mpsc::unbounded::<T>();

        // Drain: call on_event for each item. On error, rx drops and the
        // channel closes — the factory's sends start failing.
        let drain = async move {
            while let Some(item) = rx.next().await {
                let js_item = serde_wasm_bindgen::to_value(&item)
                    .map_err(|err| JsValue::from_str(&err.to_string()))?;
                on_event.call1(&JsValue::NULL, &js_item)?;
            }
            Ok::<(), JsValue>(())
        };

        // Run both concurrently. The factory's Result is the outcome; join
        // waits for the drain to finish so no items are dropped.
        let (result, _) = join(factory(tx), drain).await;
        match result {
            Ok(out) => Ok(serde_wasm_bindgen::to_value(&out).unwrap_or(JsValue::NULL)),
            Err(e) => Err(serde_wasm_bindgen::to_value(&e).unwrap_or(JsValue::NULL)),
        }
    })
}
