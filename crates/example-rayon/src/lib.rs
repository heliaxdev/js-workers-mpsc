//! Rayon-backed demo `cdylib` for the worker-bridge environment.
//!
//! Same pattern as the simple example, but the compute runs on a Rayon thread
//! pool: the JS side calls [`init_thread_pool`] once, then each export drives
//! [`js_workers_mpsc::stream_channel`] from `rayon::scope`. Senders are
//! cloned into pool workers; the receiver is drained on the worker's thread,
//! so results stream back as each pool task finishes.
//!
//! Building this crate requires nightly and the atomics flags in
//! `.cargo/config.toml` (shared memory, TLS exports); serving the demo
//! requires COOP/COEP headers for `SharedArrayBuffer`.

pub use wasm_bindgen_rayon::init_thread_pool;

mod ffi;
