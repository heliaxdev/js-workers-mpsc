//! mpsc channel bridge for streaming Rust results to a Web Worker's JS
//! callback.

mod bridge;

#[doc(inline)]
pub use bridge::{SendError, Sender, dispatch};
