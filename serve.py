#!/usr/bin/env python3
"""Serve the repo with COOP/COEP headers.

The rayon demo needs SharedArrayBuffer, which browsers only expose when the
page is served with Cross-Origin-Opener-Policy and Cross-Origin-Embedder-Policy
headers. The simple demo ignores them, so one server covers both.

Usage: python3 serve.py [port]   (default port 8000)
"""

import http.server
import sys


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
        super().end_headers()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    server = http.server.ThreadingHTTPServer(("", port), Handler)
    print(f"serving http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
