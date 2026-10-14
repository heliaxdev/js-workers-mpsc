// Bridge Worker: hosts the WASM module (instantiated exactly once) and
// relays between the Main thread and the Rust FFI boundary.
//
// Protocol (main -> worker): { id, type: "START_WORK", payload }
// Protocol (worker -> main): { id, type: "RESULT", item }
//                            { id, type: "DONE" }
//                            { id, type: "ERROR", error }

import init, { run_task } from "../pkg/js_workers_mpsc_example.js";

// Instantiate the WASM module exactly once. `ready` is a promise; every
// message handler awaits it before touching `run_task`.
const ready = init();

self.onmessage = async (e) => {
  const { id, type, payload } = e.data;

  try {
    await ready;

    if (type !== "START_WORK") {
      throw new Error(`unknown message type: ${type}`);
    }

    // `on_result` is invoked by Rust for every item streamed through the
    // mpsc channel; each call becomes one RESULT message on the main thread.
    await run_task(payload, (item) => {
      self.postMessage({ id, type: "RESULT", item });
    });

    // Resolved == all Rust senders dropped == stream complete.
    self.postMessage({ id, type: "DONE" });
  } catch (error) {
    self.postMessage({ id, type: "ERROR", error: String(error?.message ?? error) });
  }
};
