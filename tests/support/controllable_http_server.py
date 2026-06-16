"""
A small, controllable HTTP server for testing large downloads, resume and interruptions.

It serves a single source file from disk (streamed in chunks, so the memory footprint stays low
even for big files) and exposes knobs to reproduce the situations a real downloader must survive:

- ``Range`` support (``206 Partial Content`` with ``Content-Range``) for resume;
- ``ignore_range`` to emulate a server that ignores ``Range`` and re-sends the whole body (``200``);
- ``drops_remaining`` / ``drop_after`` to emulate a connection cut mid-transfer (the response
  announces the full ``Content-Length`` but only ``drop_after`` bytes are written before the socket
  is closed, so the client sees a truncated body and raises);
- ``fail_times`` to emulate transient ``503`` responses (exercising urllib3 retries);
- request/range counters for assertions.

Everything is standard library only, matching seqdd's pure-Python, cross-platform stance.
"""

from __future__ import annotations

import os
import re
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_STREAM_CHUNK = 256 * 1024  # 256 KiB server-side streaming chunk
_RANGE_RE = re.compile(r'bytes=(\d+)-(\d*)')


class _Handler(BaseHTTPRequestHandler):
    """Request handler reading its behavior from ``self.server.config`` (a ControllableHTTPServer)."""

    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):  # noqa: D401 - silence the default stderr logging
        pass

    def finish(self):
        # A simulated drop closes the socket early; ignore the resulting errors on teardown.
        try:
            super().finish()
        except (OSError, ValueError):
            pass

    @property
    def cfg(self) -> 'ControllableHTTPServer':
        return self.server.config

    def do_HEAD(self):
        cfg = self.cfg
        with cfg.lock:
            cfg.request_count += 1
            cfg.head_count += 1
        self.send_response(200)
        self.send_header('Content-Length', str(cfg.total))
        self.send_header('Accept-Ranges', 'bytes')
        if cfg.filename:
            self.send_header('Content-Disposition', f'attachment; filename="{cfg.filename}"')
        self.end_headers()

    def do_GET(self):
        cfg = self.cfg
        range_header = self.headers.get('Range')

        # Decide this request's behavior atomically.
        with cfg.lock:
            cfg.request_count += 1
            cfg.get_count += 1
            cfg.ranges.append(range_header)
            fail = cfg.fail_times > 0
            if fail:
                cfg.fail_times -= 1
            drop = (not fail) and cfg.drops_remaining > 0
            if drop:
                cfg.drops_remaining -= 1
            drop_after = cfg.drop_after
            ignore_range = cfg.ignore_range
            total = cfg.total

        if fail:
            self.send_response(503)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return

        # Parse the requested range (open-ended start is all the client ever needs).
        start = 0
        is_range = False
        if range_header and not ignore_range:
            match = _RANGE_RE.match(range_header.strip())
            if match:
                start = int(match.group(1))
                is_range = True

        if start >= total:
            # Range not satisfiable: the client already holds the whole file.
            self.send_response(416)
            self.send_header('Content-Range', f'bytes */{total}')
            self.send_header('Content-Length', '0')
            self.end_headers()
            return

        body_len = total - start
        if is_range:
            self.send_response(206)
            self.send_header('Content-Range', f'bytes {start}-{total - 1}/{total}')
        else:
            self.send_response(200)
        self.send_header('Content-Length', str(body_len))
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()

        self._stream_body(start, body_len, drop, drop_after)

    def _stream_body(self, start: int, body_len: int, drop: bool, drop_after: int) -> None:
        cfg = self.cfg
        # On a drop we write fewer bytes than the announced Content-Length, then cut the socket.
        to_send = min(drop_after, body_len) if drop else body_len
        sent = 0
        with open(cfg.source_path, 'rb') as fh:
            fh.seek(start)
            while sent < to_send:
                chunk = fh.read(min(_STREAM_CHUNK, to_send - sent))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionError, OSError):
                    return
                sent += len(chunk)

        if drop:
            # Abruptly close the connection: the client expected body_len bytes but got fewer,
            # so its streamed read raises a connection/protocol error.
            self.close_connection = True
            try:
                self.wfile.flush()
            except OSError:
                pass
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.connection.close()
            except OSError:
                pass


class ControllableHTTPServer:
    """
    A threaded HTTP server bound to an ephemeral localhost port, serving one source file.

    Use as a context manager::

        with ControllableHTTPServer(source_path) as server:
            server.drops_remaining = 1
            server.drop_after = server.total // 2
            net.download_file(server.url(), dest)
    """

    def __init__(self, source_path: str, *, filename: str | None = None):
        """
        :param source_path: Path to the file served to clients.
        :param filename: Name advertised to clients (and used by :meth:`url`); defaults to the
                         basename of ``source_path``.
        """
        self.source_path = source_path
        self.total = os.path.getsize(source_path)
        self.filename = filename or os.path.basename(source_path)

        # Behavior knobs (mutate between requests to drive scenarios).
        self.ignore_range = False
        self.drops_remaining = 0
        self.drop_after = 0
        self.fail_times = 0

        # Observability.
        self.request_count = 0
        self.get_count = 0
        self.head_count = 0
        self.ranges: list[str | None] = []

        self.lock = threading.Lock()
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> 'ControllableHTTPServer':
        self._httpd = ThreadingHTTPServer(('127.0.0.1', 0), _Handler)
        self._httpd.daemon_threads = True
        self._httpd.config = self  # the handler reads its behavior from here
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    @property
    def base_url(self) -> str:
        return f'http://127.0.0.1:{self.port}'

    def url(self, name: str | None = None) -> str:
        return f'{self.base_url}/{name or self.filename}'

    def reset_counters(self) -> None:
        with self.lock:
            self.request_count = 0
            self.get_count = 0
            self.head_count = 0
            self.ranges = []

    def __enter__(self) -> 'ControllableHTTPServer':
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
