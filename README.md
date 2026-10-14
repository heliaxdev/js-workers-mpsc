# js-workers-mpsc

Stream Rust compute from a Web Worker back to the main thread over an mpsc
channel, without blocking the page.

## Usage

The bridge glue is generic and ships with the core crate. Point a
`BridgeWorker` at any cdylib built on this crate:

```js
import { BridgeWorker } from "./crates/core/js/main.js";

const worker = new BridgeWorker({
  wasmUrl: new URL("./demo/simple/pkg/js_workers_mpsc_example.js", import.meta.url).href,
});

// event style: items flow to onmessage
worker.onmessage = (item) => console.log(item);
await worker.send(10);

// callback style
await worker.call(10, (item) => console.log(item));

// async-iterator style
for await (const item of worker.stream(10)) {
  console.log(item);
}
```

The Rust side creates a channel and sends results as they're computed:

```rust
use js_workers_mpsc::stream_channel;

#[wasm_bindgen]
pub fn run_task(payload: JsValue, on_result: js_sys::Function) -> js_sys::Promise {
    let (sender, promise) = stream_channel::<MyItem>(&on_result);
    wasm_bindgen_futures::spawn_local(async move {
        sender.unbounded_send(MyItem { /* ... */ });
    });
    promise
}
```

The same pattern works with a Rayon thread pool — clone the sender into each
pooled task. See `crates/example-rayon` and `demo/rayon/`.

## Demos

```sh
just build-simple && just serve   # http://localhost:8000/demo/simple/
just build-rayon  && just serve   # http://localhost:8000/demo/rayon/
```

## Layout

- `crates/core` — `stream_channel` plus the generic JS bridge (`js/`).
- `crates/example-simple` — demo crate: `run_task` (Fibonacci).
- `crates/example-rayon` — demo crate: `run_parallel_task` (parallel Fibonacci, Rayon pool).
- `demo/` — html pages and built wasm (`pkg/`) for each demo.

See [design.md](design.md) for the architecture and wire protocol.
