// Main-thread client. Pure JS by design: no WASM imports here.
//
//   const worker = new BridgeWorker();
//   await worker.call(8, (item) => console.log(item));   // callback style
//   for await (const item of worker.stream(10)) { ... }  // async style

export class BridgeWorker {
  #worker = new Worker(new URL("./bridge.worker.js", import.meta.url), {
    type: "module",
  });
  #seq = 0;
  #pending = new Map(); // id -> { onResult, resolve, reject }

  constructor() {
    this.#worker.onmessage = (e) => {
      const { id, type, item, error } = e.data;
      const ctx = this.#pending.get(id);
      if (!ctx) return;

      switch (type) {
        case "RESULT":
          ctx.onResult?.(item);
          break;
        case "DONE":
          this.#pending.delete(id);
          ctx.resolve();
          break;
        case "ERROR":
          this.#pending.delete(id);
          ctx.reject(new Error(error));
          break;
      }
    };
  }

  /** Raw API: per-item callback + completion Promise. */
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
