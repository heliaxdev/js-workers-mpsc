# js-workers-mpsc

Stream Rust compute from a Web Worker back to the main thread over an mpsc
channel, without blocking the page.

## Usage

Run some Rust in a worker and get results streamed back:

```js
import { BridgeWorker } from "./demo/simple/js/main.js";

const worker = new BridgeWorker();

await worker.call(10, (item) => console.log(item));

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

- `crates/core` — `stream_channel`, the channel primitive.
- `crates/example-simple` — demo: `run_task` (Fibonacci).
- `crates/example-rayon` — demo: `run_parallel_task` (parallel Fibonacci, Rayon pool).
- `demo/` — the JS workers, main thread clients, and html.

See [design.md](design.md) for the architecture and wire protocol.
