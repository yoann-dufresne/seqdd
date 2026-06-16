"""
Minimal, dependency-free progress reporting for the download pipeline.

On an interactive terminal the caller draws a single, in-place line (rewritten with a carriage
return) that combines a finished-jobs counter (``x/n``) with a byte progress bar for the files being
downloaded (the bar is filled by bytes downloaded / total). On a non-interactive stream (CI logs,
pipes, redirections) the caller keeps emitting plain periodic log lines instead, so logs are never
polluted with carriage-return spam.

This module provides the pure line formatters (:func:`format_byte_progress`,
:func:`format_jobs_line`, :func:`human_bytes`) and the in-place line writer (:class:`ProgressBar`).
Everything is standard library only (no ``tqdm``), preserving seqdd's single third-party runtime
dependency (``requests``).
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


def format_byte_progress(downloaded: int, total: int, *, width: int = 30,
                         elapsed: float | None = None, suffix: str = '') -> str:
    """
    Build a byte-based progress bar whose fill reflects ``downloaded``/``total``.

    Pure function (no I/O, no clock), so it is directly unit-testable.

    :param downloaded: Bytes downloaded so far.
    :param total: Total bytes expected (the bar is empty when this is 0).
    :param width: The width of the bar (in characters).
    :param elapsed: An optional elapsed time in seconds, appended when provided.
    :param suffix: Optional trailing text appended after the percentage.
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


def format_jobs_line(done: int, total: int, *, downloaded: int = 0,
                     elapsed: float | None = None) -> str:
    """
    Build the counter line shown while no byte total is known yet — a plain ``x/n`` counter, no bar.

    Used at the very start of a run, and for downloads whose size the server does not announce
    (e.g. chunked responses): the bytes pulled so far are shown as a throughput counter instead.

    :param done: The number of finished jobs.
    :param total: The total number of jobs.
    :param downloaded: Bytes downloaded so far (shown as a throughput counter when > 0).
    :param elapsed: An optional elapsed time in seconds, appended when provided.
    :return: A one-line string, e.g. ``3/8 jobs  12.0 MiB  5s``.
    """
    text = f'{done}/{total} jobs'
    if downloaded:
        text += f'  {human_bytes(downloaded)}'
    if elapsed is not None:
        text += f' {elapsed:.0f}s'
    return text


class ProgressBar:
    """
    A live, single-line, in-place writer for a pre-formatted progress line.

    On an interactive terminal it rewrites one line in place (carriage return + clear-to-end of
    line); on a non-interactive stream it is a no-op (:attr:`active` is False) so the caller can
    fall back to logging. The line content is composed by the caller (see :func:`format_byte_progress`
    and :func:`format_jobs_line`).
    """

    def __init__(self, *, stream: TextIO | None = None) -> None:
        """
        :param stream: The stream to draw on (defaults to :data:`sys.stderr`).
        """
        self.stream = stream if stream is not None else sys.stderr
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
        Draw a pre-formatted line in place (no-op off a TTY or after :meth:`finish`).

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
