# Design

Three layers, one direction of data flow:

```
┌──────────────────────────────────┐
│  Main thread                     │  pure JS, no WASM imports
│  (crates/core/js/main.js)        │
│  BridgeWorker                    │
└──────────┬───────────▲───────────┘
           │ postMessage { id, type, payload }
           ▼           │ RESULT / DONE / ERROR
┌──────────────────────────────────┐
│  Bridge Worker                   │  WASM module instantiated exactly once
│  (crates/core/js/                │  generic: parameterized by an INIT
│   bridge.worker.js)              │  message, shared by all demos
└──────────┬───────────────────────┘
           │ run_task / run_parallel_task(payload, on_result)
┌──────────▼───────────────────────┐
│  Rust compute                    │  runs on the Worker's thread
│  (crates/example-*/src/ffi.rs)   │  streams items through the channel
└──────────────────────────────────┘
```

Two demos share this shape. The bridge glue is generic and lives in
`crates/core/js/`; the demos differ only in what the Rust compute does and
in the options they pass to `BridgeWorker`:

| | `demo/simple` | `demo/rayon` |
| --- | --- | --- |
| Crate | `crates/example-simple` | `crates/example-rayon` |
| Export | `run_task` | `run_parallel_task` + `init_thread_pool` |
| `BridgeWorker` options | (defaults) | `taskName: "runParallelTask"`, `initThreadPool: true` |
| Compute | on the worker's own thread | across a Rayon thread pool |
| Build | stable or nightly | nightly + atomics (`.cargo/config.toml` in the crate) |
| Serving | any static server | requires COOP/COEP headers (SharedArrayBuffer) |

## Wire protocol

| Direction | Message | Meaning |
| --- | --- | --- |
| main → worker | `{ type: "INIT", wasmUrl, taskName, initThreadPool, numThreads }` | parameterize the worker: which WASM module, which task export, optional Rayon pool init |
| main → worker | `{ id, type: "START_WORK", payload }` | run a task |
| worker → main | `{ id, type: "RESULT", item }` | one streamed item |
| worker → main | `{ id, type: "DONE" }` | stream complete |
| worker → main | `{ id, type: "ERROR", error }` | stream failed |

`INIT` is posted by the `BridgeWorker` constructor; the worker dynamically
imports `wasmUrl`, instantiates the module once, optionally spins up the
Rayon pool, and queues any `START_WORK` that arrives before init finishes.

Each request carries an `id` so multiple streams can run concurrently on one
worker without their results interleaving.

The main-thread `BridgeWorker` surfaces three styles over this protocol,
routing per stream `id` (so they never double-deliver):

- `worker.call(payload, onResult)` — per-item callback + completion Promise.
- `for await (const item of worker.stream(payload))` — async iterator.
- `worker.onmessage = (item) => …` + `worker.send(payload)` — event style;
  `onmessage` receives each channel item directly, `onerror` fires on
  stream failure, and `send` returns a Promise that settles with the stream.

### The bridge contract

The JS glue in `crates/core/js/` ships with the core crate and is generic:
it bridges any cdylib that exports a task function with the shape
`run_task(payload, on_result) -> Promise` (JS export name configurable via
`taskName`, default `runTask`). `on_result` is invoked once per item streamed
through the channel; the returned `Promise` resolves when all senders drop,
which the worker forwards as `DONE`. A demo is then just an `index.html`
that constructs `BridgeWorker` with the `wasmUrl` of its `pkg/` plus any
options — there is no per-demo JS glue to write or keep in sync.

## The channel

`stream_channel` pairs an unbounded mpsc `Sender<T>` with a JS callback. The
receiver is drained on the worker's thread by `wasm-bindgen-futures`, so the
JS event loop is never blocked. Each item is serialized with
`serde-wasm-bindgen` and passed to the callback. When all senders drop, the
returned `Promise` resolves.

`stream_channel` is generic, so it can't be exported through `wasm-bindgen`
directly. Each concrete task needs its own `#[wasm_bindgen]` export that
fixes the type parameter and drives the channel. Both example crates are
such exports.

The sender is `Send + Sync + Clone`, which is what makes the Rayon variant
work with no changes to the core: the export clones the sender into each
`rayon::scope` task, and pool threads push results with the synchronous
`unbounded_send`. The receiver's drain loop wakes on its own; no
`Atomics.wait` or manual yielding is needed. Results arrive in pool
completion order rather than index order.

## Why a worker

Web Workers can block. The main thread can't. Putting the WASM module in a
worker means the Rust side can do long-running or synchronous work without
janking the UI, and the channel is how results get back out.

## No thread pool (simple demo)

The simple demo is single-threaded. Compute runs on the worker's own thread,
and no `SharedArrayBuffer` is involved, so it serves from any static server
without COOP/COEP headers.

## Rayon thread pool (rayon demo)

The rayon demo adds `wasm-bindgen-rayon`. Three things change:

- The bridge worker calls `initThreadPool` once, right after module init,
  when constructed with `initThreadPool: true` (thread count defaults to
  `navigator.hardwareConcurrency`). Pool workers re-import the same WASM
  module and share its linear memory (`SharedArrayBuffer`).
- The build needs nightly, `build-std`, and atomics link flags. These live in
  `crates/example-rayon/.cargo/config.toml` so the simple crate is unaffected;
  the rayon crate must be built from its own directory so cargo picks that
  config up, with `RUSTFLAGS` unset so the config wins. The build also patches
  the generated `workerHelpers.js` (the `just build-rayon` recipe applies the
  patch): its dynamic `import('../../..')` resolves to the `pkg/` directory,
  which bundlers handle via `package.json` `main` but native ES modules
  cannot, so it is rewritten to the main module file.
- Serving requires COOP/COEP headers, because `SharedArrayBuffer` is
  cross-origin-isolated only under those headers. `serve.py` at the repo
  root sets them; a plain static server is enough for the simple demo.

The pool tasks hold only cloned senders, never the receiver; the drain loop
stays on the bridge worker's thread. `initThreadPool` runs exactly once,
before the first task.
