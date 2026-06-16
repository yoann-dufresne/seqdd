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


def human_bytes(n: int) -> str:
    """
    Format a byte count with a binary unit (B, KiB, MiB, …).

    :param n: A number of bytes.
    :return: A short human-readable string, e.g. ``12.3 MiB`` (or ``512 B`` for small values).
    """
    size = float(max(n, 0))
    units = ('B', 'KiB', 'MiB', 'GiB', 'TiB')
    for unit in units[:-1]:
        if size < 1024.0:
            return f'{size:.0f} {unit}' if unit == 'B' else f'{size:.1f} {unit}'
        size /= 1024.0
    return f'{size:.1f} {units[-1]}'


def format_progress(done: int, total: int, *, width: int = 30, failed: int = 0,
                    elapsed: float | None = None, extra: str = '') -> str:
    """
    Build the textual job-count progress bar for ``done``/``total`` finished jobs.

    Pure function (no I/O, no clock): for the same inputs it always returns the same string, which
    makes it directly unit-testable.

    :param done: The number of finished jobs.
    :param total: The total number of jobs.
    :param width: The width of the bar (in characters).
    :param failed: The number of failed/canceled jobs to flag (omitted when 0).
    :param elapsed: An optional elapsed time in seconds, appended when provided.
    :param extra: Optional extra text (e.g. bytes downloaded) appended after the percentage.
    :return: A one-line progress string, e.g. ``[###############---------------] 5/10 jobs (50%)``.
    """
    total = max(total, 0)
    done = max(0, min(done, total)) if total else max(done, 0)
    fraction = (done / total) if total else 1.0
    filled = int(fraction * width)
    bar = '#' * filled + '-' * (width - filled)
    percent = int(fraction * 100)
    text = f'[{bar}] {done}/{total} jobs ({percent}%)'
    if extra:
        text += extra
    if elapsed is not None:
        text += f' {elapsed:.0f}s'
    if failed:
        text += f' - {failed} failed'
    return text


def format_byte_progress(downloaded: int, total: int, *, width: int = 30,
                         elapsed: float | None = None, suffix: str = '') -> str:
    """
    Build a byte-based progress bar whose fill reflects ``downloaded``/``total``.

    Pure function (no I/O, no clock), so it is directly unit-testable.

    :param downloaded: Bytes downloaded so far.
    :param total: Total bytes expected (the bar is empty when this is 0).
    :param width: The width of the bar (in characters).
    :param elapsed: An optional elapsed time in seconds, appended when provided.
    :param suffix: Optional trailing text (e.g. a job count) appended after the percentage.
    :return: A one-line string, e.g. ``[############------------------] 12.0 MiB / 30.0 MiB (40%)``.
    """
    fraction = (downloaded / total) if total > 0 else 0.0
    fraction = min(max(fraction, 0.0), 1.0)
    filled = int(fraction * width)
    bar = '#' * filled + '-' * (width - filled)
    percent = int(fraction * 100)
    text = f'[{bar}] {human_bytes(downloaded)} / {human_bytes(total)} ({percent}%)'
    if suffix:
        text += f'  {suffix}'
    if elapsed is not None:
        text += f' {elapsed:.0f}s'
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

    @property
    def elapsed(self) -> float:
        """
        :return: Seconds elapsed since the bar was created.
        """
        return time.monotonic() - self._start

    def draw(self, line: str) -> None:
        """
        Draw an arbitrary, pre-formatted line in place (no-op off a TTY or after :meth:`close`).

        :param line: The fully-formatted line to render.
        """
        if not self.active or self._closed:
            return
        self.stream.write(f'\r{line}\x1b[K')
        self.stream.flush()

    def finish(self, line: str) -> None:
        """
        Draw a final, pre-formatted line and move to a new line. Idempotent.

        :param line: The fully-formatted line to render.
        """
        if self._closed:
            return
        self._closed = True
        if not self.active:
            return
        self.stream.write(f'\r{line}\x1b[K\n')
        self.stream.flush()

    def update(self, done: int, failed: int = 0, extra: str = '') -> None:
        """
        Redraw the bar in place (no-op on a non-interactive stream or after :meth:`close`).

        :param done: The number of finished jobs.
        :param failed: The number of failed/canceled jobs.
        :param extra: Optional extra text (e.g. bytes downloaded) appended after the percentage.
        """
        if not self.active or self._closed:
            return
        line = format_progress(done, self.total, width=self.width, failed=failed,
                               elapsed=time.monotonic() - self._start, extra=extra)
        # Pad to clear any leftovers from a previously longer line, then return to column 0.
        self.stream.write(f'\r{line}\x1b[K')
        self.stream.flush()

    def close(self, done: int, failed: int = 0, extra: str = '') -> None:
        """
        Draw the final state and move to a new line. Idempotent.

        :param done: The number of finished jobs.
        :param failed: The number of failed/canceled jobs.
        :param extra: Optional extra text (e.g. bytes downloaded) appended after the percentage.
        """
        if self._closed:
            return
        self._closed = True
        if not self.active:
            return
        line = format_progress(done, self.total, width=self.width, failed=failed,
                               elapsed=time.monotonic() - self._start, extra=extra)
        self.stream.write(f'\r{line}\x1b[K\n')
        self.stream.flush()
