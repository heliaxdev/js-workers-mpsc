//! mpsc channel bridge for streaming Rust results to a Web Worker's JS
//! callback.
//!
//! [`stream_channel`] pairs an mpsc [`Sender<T>`] with a JavaScript
//! function and a [`Promise`][js_sys::Promise]. Each item sent on the sender
//! is serialized with `serde-wasm-bindgen` and passed to the callback; the
//! promise resolves when the sender is dropped, or rejects if the callback
//! throws or serialization fails.
//!
//! This is an `rlib` with no `#[wasm_bindgen]` exports. A downstream `cdylib`
//! owns the exports the worker loads — see `crates/example-simple` and
//! `crates/example-rayon` for references.

mod bridge;

pub use bridge::{Sender, TrySendError, stream_channel};
