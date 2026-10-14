# Build the simple demo (stable or nightly, no atomics).
build-simple:
    wasm-pack build --target web crates/example-simple -d ../../demo/simple/pkg

# Build the rayon demo (nightly + atomics). Patches the generated
# workerHelpers.js so its dynamic import resolves to the main module file
# instead of the pkg/ directory — native ESM can't import a directory, and
# we serve statically (no bundler to resolve package.json `main`).
build-rayon:
    cd crates/example-rayon && env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS wasm-pack build --target web -d ../../demo/rayon/pkg
    sed -i.bak "s|import('../../..')|import('../../../js_workers_mpsc_example_rayon.js')|" demo/rayon/pkg/snippets/*/src/workerHelpers.js && rm -f demo/rayon/pkg/snippets/*/src/workerHelpers.js.bak

# Serve both demos with COOP/COEP headers (required for the rayon demo's
# SharedArrayBuffer; harmless to the simple one).
serve port="8000":
    python3 serve.py {{port}}
