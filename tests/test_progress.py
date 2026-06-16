"""
Unit tests for the dependency-free progress reporting (``seqdd.utils.progress``).

The pure formatters (:func:`human_bytes`, :func:`format_byte_progress`, :func:`format_jobs_line`)
are checked for exact, deterministic output, and :class:`ProgressBar` for its TTY/non-TTY in-place
writing behavior using in-memory streams.
"""

import io

from tests import SeqddTest

from seqdd.utils.progress import ProgressBar, format_byte_progress, format_jobs_line, human_bytes


class _FakeTTY(io.StringIO):
    """An in-memory stream that pretends to be an interactive terminal."""

    def isatty(self) -> bool:
        return True


class TestHumanBytes(SeqddTest):

    def test_bytes(self):
        self.assertEqual(human_bytes(0), '0 B')
        self.assertEqual(human_bytes(512), '512 B')

    def test_scales_up_binary_units(self):
        self.assertEqual(human_bytes(1024), '1.0 KiB')
        self.assertEqual(human_bytes(1536), '1.5 KiB')
        self.assertEqual(human_bytes(5 * 1024 * 1024), '5.0 MiB')
        self.assertEqual(human_bytes(1 << 30), '1.0 GiB')


class TestFormatByteProgress(SeqddTest):

    def test_empty(self):
        self.assertEqual(format_byte_progress(0, 100, width=10), '[----------] 0 B / 100 B (0%)')

    def test_half(self):
        self.assertEqual(format_byte_progress(50, 100, width=10), '[#####-----] 50 B / 100 B (50%)')

    def test_full(self):
        self.assertEqual(format_byte_progress(100, 100, width=10), '[##########] 100 B / 100 B (100%)')

    def test_fill_reflects_bytes(self):
        # The whole point: a download at 40% of its bytes shows 40%, regardless of job count.
        self.assertEqual(format_byte_progress(4 * 1024 * 1024, 10 * 1024 * 1024, width=10),
                         '[####------] 4.0 MiB / 10.0 MiB (40%)')

    def test_overflow_is_clamped(self):
        self.assertEqual(format_byte_progress(150, 100, width=10), '[##########] 150 B / 100 B (100%)')

    def test_zero_total_is_empty(self):
        self.assertEqual(format_byte_progress(0, 0, width=10), '[----------] 0 B / 0 B (0%)')

    def test_suffix_and_elapsed(self):
        line = format_byte_progress(50, 100, width=10, suffix='2/4 jobs', elapsed=7.0)
        self.assertTrue(line.endswith('(50%)  2/4 jobs 7s'))


class TestFormatJobsLine(SeqddTest):

    def test_counter_only(self):
        self.assertEqual(format_jobs_line(3, 8), '3/8 jobs')

    def test_with_throughput(self):
        self.assertEqual(format_jobs_line(3, 8, downloaded=5 * 1024 * 1024), '3/8 jobs  5.0 MiB')

    def test_with_elapsed(self):
        self.assertEqual(format_jobs_line(0, 8, elapsed=4.0), '0/8 jobs 4s')

    def test_is_a_plain_counter_not_a_bar(self):
        # No bar characters: the job count must never be rendered as a filled bar.
        self.assertNotIn('[', format_jobs_line(2, 8, downloaded=1024))


class TestProgressBar(SeqddTest):

    def test_draw_writes_line_in_place(self):
        stream = _FakeTTY()
        bar = ProgressBar(stream=stream)
        bar.draw('hello')
        self.assertEqual(stream.getvalue(), '\rhello\x1b[K')

    def test_finish_terminates_and_is_idempotent(self):
        stream = _FakeTTY()
        bar = ProgressBar(stream=stream)
        bar.finish('done')
        first = stream.getvalue()
        self.assertEqual(first, '\rdone\x1b[K\n')
        bar.finish('again')   # no-op after finish
        bar.draw('again')     # no-op after finish
        self.assertEqual(stream.getvalue(), first)

    def test_noop_off_tty(self):
        stream = io.StringIO()  # StringIO.isatty() is False
        bar = ProgressBar(stream=stream)
        self.assertFalse(bar.active)
        bar.draw('hello')
        bar.finish('done')
        self.assertEqual(stream.getvalue(), '')
