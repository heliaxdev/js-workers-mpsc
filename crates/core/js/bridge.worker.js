// Generic bridge worker: hosts a WASM module (instantiated exactly once) and
// relays between the main thread and the Rust FFI boundary. Parameterized by
// an INIT message rather than a hardcoded import, so this same file bridges
// any cdylib built on this crate.
//
// Protocol (main -> worker): { type: "INIT", wasmUrl, taskName,
//                              initThreadPool, numThreads }
//                            { id, type: "START_WORK", payload }
// Protocol (worker -> main): { id, type: "RESULT", item }
//                            { id, type: "DONE" }
//                            { id, type: "ERROR", error }
//
// The bridged cdylib must export a task function
// `<taskName>(payload, onResult) -> Promise` (default name: `runTask`; Rust:
// `run_task(payload, on_result) -> Promise`). `onResult` is invoked by Rust
// for every item streamed through the mpsc channel; each call becomes one
// RESULT message on the main thread. The task's Promise resolves when all
// Rust senders drop, which becomes DONE.

let runTask = null;
let initError = null;

// The port delivers INIT before any START_WORK, but delivery order is not
// completion order: INIT suspends at its first `await`, and the event loop
// dispatches the next message while it is still pending. START_WORK landing
// in that window is parked here until INIT finishes and drains the array.
const queued = [];

self.onmessage = async (e) => {
  const { type } = e.data;

  if (type === "INIT") {
    const { wasmUrl, taskName = "runTask", initThreadPool, numThreads } = e.data;
    try {
      const wasm = await import(wasmUrl);
      await wasm.default();
      if (initThreadPool) {
        await wasm.initThreadPool(numThreads ?? navigator.hardwareConcurrency);
      }
      runTask = wasm[taskName];
      queued.splice(0).forEach(handle);
    } catch (error) {
      initError = String(error?.message ?? error);
      queued.splice(0).forEach((msg) => fail(msg));
    }
    return;
  }

  if (type === "START_WORK") {
    if (runTask) handle(e.data);
    else if (initError) fail(e.data);
    else queued.push(e.data);
  }
};

function handle({ id, payload }) {
  Promise.resolve(
    runTask(payload, (item) => {
      self.postMessage({ id, type: "RESULT", item });
    }),
  )
    .then(() => self.postMessage({ id, type: "DONE" }))
    .catch((error) => fail({ id }, String(error?.message ?? error)));
}

function fail({ id }, error = initError) {
  self.postMessage({ id, type: "ERROR", error });
}
