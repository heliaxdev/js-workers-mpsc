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
//
//   worker.close();     // soft stop: no new work, in-flight drains
//   worker.terminate(); // hard kill

const closedError = () =>
  new DOMException("bridge worker closed", "AbortError");

export class BridgeWorker {
  #worker;
  #seq = 0;
  #closed = false;
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
    workerUrl,
  }) {
    this.#worker = new Worker(
      // Defaults to bridge.worker.js next to this file; overridable for
      // setups (bundlers, alternate layouts) where the two can't be served
      // side by side.
      workerUrl ?? new URL("./bridge.worker.js", import.meta.url),
      { type: "module" },
    );

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

  // Start a stream under a fresh id. After close() the promise rejects
  // without ever reaching the worker.
  #start(payload, onResult) {
    const id = this.#seq++;
    if (this.#closed) {
      return { id, promise: Promise.reject(closedError()) };
    }
    const promise = new Promise((resolve, reject) => {
      this.#pending.set(id, { onResult, resolve, reject });
    });
    this.#worker.postMessage({ id, type: "START_WORK", payload });
    return { id, promise };
  }

  // Stop tracking a stream: remaining items are ignored and the worker is
  // told to drop them. A no-op once the stream has ended (or never started).
  #abandon(id) {
    if (!this.#pending.has(id)) return;
    this.#pending.delete(id);
    this.#worker.postMessage({ id, type: "CANCEL" });
  }

  /**
   * Event API: items flow to `onmessage`; resolves on DONE, rejects on ERROR.
   * Concurrent `send`s interleave their items on the one `onmessage`.
   */
  send(payload) {
    return this.#start(payload, null).promise;
  }

  /** Callback API: per-item `onResult` + completion Promise. */
  call(payload, onResult) {
    return this.#start(payload, onResult).promise;
  }

  /**
   * Async API: `for await (const item of worker.stream(payload))`.
   * Breaking out early abandons the stream; remaining items are dropped.
   */
  async *stream(payload) {
    const queue = [];
    let wake;
    let done = false;
    let err = null;

    const { id, promise } = this.#start(payload, (item) => {
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

    try {
      while (true) {
        if (err) throw err;
        if (queue.length) {
          yield queue.shift();
          continue;
        }
        if (done) return;
        await new Promise((r) => (wake = r));
      }
    } finally {
      // Runs on early break/throw too: the consuming loop closes the
      // generator. A no-op when the stream already ended.
      this.#abandon(id);
    }
  }

  /** Soft stop: new work rejects with an AbortError; in-flight streams drain
   *  normally. The worker thread keeps running (terminate() kills it). */
  close() {
    this.#closed = true;
  }

  terminate() {
    this.#worker.terminate();
    // Nothing will ever settle these streams now; reject rather than hang.
    for (const { reject } of this.#pending.values()) {
      reject(new DOMException("worker terminated", "AbortError"));
    }
    this.#pending.clear();
  }
}
