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
| main → worker | `{ id, type: "CANCEL" }` | abandon a stream: the worker drops its remaining items (the Rust compute still runs) |
| worker → main | `{ id, type: "RESULT", item }` | one streamed item |
| worker → main | `{ id, type: "DONE" }` | stream complete |
| worker → main | `{ id, type: "ERROR", error }` | stream failed |

`INIT` is posted by the `BridgeWorker` constructor; the worker dynamically
imports `wasmUrl`, instantiates the module once, optionally spins up the
Rayon pool, and queues any `START_WORK` that arrives before init finishes.
If init fails, every queued (and later) `START_WORK` gets an `ERROR`
with the init failure.

Each request carries an `id` so multiple streams can run concurrently on one
worker without their results interleaving.

The main-thread `BridgeWorker` surfaces three styles over this protocol,
routing per stream `id` (so they never double-deliver):

- `worker.call(payload, onResult)` — per-item callback + completion Promise.
- `for await (const item of worker.stream(payload))` — async iterator.
- `worker.onmessage = (item) => …` + `worker.send(payload)` — event style;
  `onmessage` receives each channel item directly, `onerror` fires on
  stream failure, and `send` returns a Promise that settles with the stream.

Breaking out of a `stream()` loop abandons that stream: the main thread
deletes its routing entry and posts `CANCEL`, so remaining items are dropped
at the worker instead of delivered. The Rust compute still runs to
completion — the sender is owned by the spawned future — only its
forwarding stops.

`worker.close()` is the soft stop: new `send`/`call`/`stream` calls reject
with an `AbortError`, while streams already in flight drain normally.
`worker.terminate()` is the hard kill and rejects in-flight streams.

### The bridge contract

The JS glue in `crates/core/js/` ships with the core crate and is generic:
it bridges any cdylib that exports a task function with the shape
`run_task(payload, on_result) -> Promise` (JS export name configurable via
`taskName`, default `run_task` — the Rust name as-is; wasm-bindgen no
longer camelCases export names). `on_result` is invoked once per item streamed
through the channel; the returned `Promise` settles with the task's
`Result`: resolve forwards as `DONE`, reject (the Rust `Err`, serialized)
forwards as `ERROR` for JS to catch and restart. A demo is then just an `index.html`
that constructs `BridgeWorker` with the `wasmUrl` of its `pkg/` plus any
options — there is no per-demo JS glue to write or keep in sync. The worker
script itself defaults to `bridge.worker.js` next to `main.js`; `workerUrl`
overrides it for setups where the two files can't be served side by side
(bundlers, alternate layouts).

## The channel

`dispatch` runs a factory future that streams items to a JS callback. The
factory receives the sending end of an unbounded mpsc channel and returns
`Result<Out, E>`; each item sent is serialized with `serde-wasm-bindgen`
and passed to `on_event`. The drain loop and the factory run concurrently
on the worker's thread via `wasm-bindgen-futures`, so the JS event loop is
never blocked. The factory's `Result` becomes the promise outcome: `Ok`
resolves, `Err` rejects (serialized), which is what JS catches to restart
the process.

When `on_event` throws (or an item fails to serialize), the receiver drops
and the channel closes. The factory learns this through `send`: subsequent
sends error, and the factory propagates that as its `Err`. The factory's
`Result` is the only source of "why this stopped" — the drain's own error
is lost if the factory ignores failed sends.

`dispatch` is generic, so it can't be exported through `wasm-bindgen`
directly. Each concrete task needs its own `#[wasm_bindgen]` export that
fixes the type parameters and hands the channel to the factory. Both
example crates are such exports.

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

## Known limitations

- **Abandoned streams keep computing.** Breaking out of a `stream()` loop or
  closing the worker stops forwarding results, but the Rust future runs to
  completion: the sender is owned by the spawned future and can't be dropped
  from JS. True mid-compute cancellation needs Rust-side cooperation (a
  closable sender or shared flag).
- **No backpressure.** The mpsc is unbounded and the main-thread queue is
  unbounded; a fast producer outruns a slow consumer without limit.
- **Panics are not handled.** A panic in factory code (or in a pool task)
  aborts the worker before any `ERROR` can be posted; awaiting callers
  hang. There is no detection or recovery — `catch_unwind` doesn't work
  under `panic = "abort"`. The user is responsible for not panicking;
  make fallible compute return `Result` instead.
