"""Pass-through proxy in front of the REST catalog that prints what Sail sends in CreateTableRequest.

Usage: proxy.py <listen port> <upstream base url>
"""

import json
import sys
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port, upstream = int(sys.argv[1]), sys.argv[2]


class Proxy(BaseHTTPRequestHandler):
    def _forward(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0)) or None
        if self.command == "POST" and self.path.endswith("/tables") and body:
            req = json.loads(body)
            print(
                f"CreateTableRequest {req['name']!r}: location={req.get('location')!r}",
                flush=True,
            )
        headers = {
            k: v
            for k, v in self.headers.items()
            if k.lower() not in ("host", "content-length")
        }
        request = urllib.request.Request(
            upstream + self.path, data=body, method=self.command, headers=headers
        )
        try:
            with urllib.request.urlopen(request) as r:
                status, data, ctype = r.status, r.read(), r.headers.get("Content-Type")
        except urllib.error.HTTPError as e:
            status, data, ctype = e.code, e.read(), e.headers.get("Content-Type")
        self.send_response(status)
        if ctype:
            self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_DELETE = do_HEAD = _forward

    def log_message(self, *args) -> None:  # keep output to the CreateTableRequest lines
        pass


ThreadingHTTPServer(("127.0.0.1", port), Proxy).serve_forever()
