// Main-thread client. Pure JS by design: no WASM imports here. The bridge
// worker glue this class pairs with lives next to it (bridge.worker.js) and
// is generic: point it at any cdylib built on this crate.
//
//   const worker = new BridgeWorker({
//     wasmUrl: new URL("./pkg/my_crate.js", import.meta.url).href,
//   });
//
//   worker.onmessage = (item) => console.log(item);   // event style
//   await worker.send(10);
//
//   await worker.call(8, (item) => console.log(item));   // callback style
//
//   for await (const item of worker.stream(10)) { ... }  // async style

export class BridgeWorker {
  #worker = new Worker(new URL("./bridge.worker.js", import.meta.url), {
    type: "module",
  });
  #seq = 0;
  #pending = new Map(); // id -> { onResult, resolve, reject }

  // Event handlers for `send`-initiated streams. Items are the values
  // delivered to the Rust channel, passed through as-is.
  onmessage = null; // (item) => void
  onerror = null; // (err) => void

  constructor({
    wasmUrl,
    taskName = "runTask",
    initThreadPool = false,
    numThreads,
  }) {
    this.#worker.onmessage = (e) => {
      const { id, type, item, error } = e.data;
      const ctx = this.#pending.get(id);
      if (!ctx) return;

      switch (type) {
        case "RESULT":
          if (ctx.onResult) ctx.onResult(item);
          else this.onmessage?.(item);
          break;
        case "DONE":
          this.#pending.delete(id);
          ctx.resolve();
          break;
        case "ERROR":
          this.#pending.delete(id);
          this.#fail(ctx, error);
          break;
      }
    };

    this.#worker.postMessage({
      type: "INIT",
      wasmUrl,
      taskName,
      initThreadPool,
      numThreads,
    });
  }

  #fail(ctx, error) {
    const err = new Error(error);
    ctx.reject(err);
    if (!ctx.onResult) this.onerror?.(err);
  }

  /** Event API: items flow to `onmessage`; resolves on DONE, rejects on ERROR. */
  send(payload) {
    const id = this.#seq++;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { onResult: null, resolve, reject });
      this.#worker.postMessage({ id, type: "START_WORK", payload });
    });
  }

  /** Callback API: per-item `onResult` + completion Promise. */
  call(payload, onResult) {
    const id = this.#seq++;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { onResult, resolve, reject });
      this.#worker.postMessage({ id, type: "START_WORK", payload });
    });
  }

  /** Async API: `for await (const item of worker.stream(payload))`. */
  async *stream(payload) {
    const queue = [];
    let wake;
    let done = false;
    let err = null;

    const promise = this.call(payload, (item) => {
      queue.push(item);
      wake?.();
    });

    promise.then(() => {
      done = true;
      wake?.();
    });
    promise.catch((e) => {
      err = e;
      wake?.();
    });

    while (true) {
      if (err) throw err;
      if (queue.length) {
        yield queue.shift();
        continue;
      }
      if (done) return;
      await new Promise((r) => (wake = r));
    }
  }

  terminate() {
    this.#worker.terminate();
    this.#pending.clear();
  }
}
