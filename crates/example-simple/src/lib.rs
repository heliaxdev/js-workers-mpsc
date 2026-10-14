//! Demo `cdylib` for the worker-bridge environment.
//!
//! Owns the `#[wasm_bindgen]` exports the bridge worker loads. Each export is
//! a thin wrapper around [`js_workers_mpsc::stream_channel`] that drives the
//! channel from a `spawn_local` future.

mod ffi;
