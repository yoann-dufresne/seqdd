"""
Minimal, dependency-free progress reporting for the download job queue.

The download pipeline runs many jobs in parallel and each job already writes its own output to a
dedicated log file. The only thing worth showing live on the console is therefore *how many jobs
are finished*. This module renders that as a single, in-place line on an interactive terminal
(rewritten with a carriage return) and stays silent on non-interactive streams (CI logs, pipes,
redirections), where the caller keeps emitting plain periodic log lines instead — so logs are
never polluted with carriage-return spam.

Everything here is standard library only (no ``tqdm``), preserving seqdd's single third-party
runtime dependency (``requests``).
"""

from __future__ import annotations

import sys
import time
from typing import TextIO


def format_progress(done: int, total: int, *, width: int = 30, failed: int = 0,
                    elapsed: float | None = None) -> str:
    """
    Build the textual job-count progress bar for ``done``/``total`` finished jobs.

    Pure function (no I/O, no clock): for the same inputs it always returns the same string, which
    makes it directly unit-testable.

    :param done: The number of finished jobs.
    :param total: The total number of jobs.
    :param width: The width of the bar (in characters).
    :param failed: The number of failed/canceled jobs to flag (omitted when 0).
    :param elapsed: An optional elapsed time in seconds, appended when provided.
    :return: A one-line progress string, e.g. ``[###############---------------] 5/10 jobs (50%)``.
    """
    total = max(total, 0)
    done = max(0, min(done, total)) if total else max(done, 0)
    fraction = (done / total) if total else 1.0
    filled = int(fraction * width)
    bar = '#' * filled + '-' * (width - filled)
    percent = int(fraction * 100)
    text = f'[{bar}] {done}/{total} jobs ({percent}%)'
    if elapsed is not None:
        text += f' {elapsed:.0f}s'
    if failed:
        text += f' - {failed} failed'
    return text


class ProgressBar:
    """
    A live, single-line job-count progress bar.

    On an interactive terminal it rewrites one line in place; on a non-interactive stream it is a
    no-op (:attr:`active` is False) so the caller can fall back to logging.
    """

    def __init__(self, total: int, *, stream: TextIO | None = None, width: int = 30) -> None:
        """
        :param total: The total number of jobs to track.
        :param stream: The stream to draw on (defaults to :data:`sys.stderr`).
        :param width: The width of the bar (in characters).
        """
        self.total = total
        self.stream = stream if stream is not None else sys.stderr
        self.width = width
        self._start = time.monotonic()
        self._closed = False

    @property
    def active(self) -> bool:
        """
        :return: True when the stream is an interactive terminal worth drawing on.
        """
        return bool(getattr(self.stream, 'isatty', lambda: False)())

    def update(self, done: int, failed: int = 0) -> None:
        """
        Redraw the bar in place (no-op on a non-interactive stream or after :meth:`close`).

        :param done: The number of finished jobs.
        :param failed: The number of failed/canceled jobs.
        """
        if not self.active or self._closed:
            return
        line = format_progress(done, self.total, width=self.width, failed=failed,
                               elapsed=time.monotonic() - self._start)
        self.stream.write(f'\r{line}')
        self.stream.flush()

    def close(self, done: int, failed: int = 0) -> None:
        """
        Draw the final state and move to a new line. Idempotent.

        :param done: The number of finished jobs.
        :param failed: The number of failed/canceled jobs.
        """
        if self._closed:
            return
        self._closed = True
        if not self.active:
            return
        line = format_progress(done, self.total, width=self.width, failed=failed,
                               elapsed=time.monotonic() - self._start)
        self.stream.write(f'\r{line}\n')
        self.stream.flush()
