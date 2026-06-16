"""
Unit tests for the dependency-free progress reporting (``seqdd.utils.progress``).

The pure :func:`format_progress` renderer is checked for exact, deterministic output, and
:class:`ProgressBar` is checked for its TTY/non-TTY behavior using in-memory streams.
"""

import io

from tests import SeqddTest

from seqdd.utils.progress import ProgressBar, format_byte_progress, format_progress, human_bytes


class _FakeTTY(io.StringIO):
    """An in-memory stream that pretends to be an interactive terminal."""

    def isatty(self) -> bool:
        return True


class TestFormatProgress(SeqddTest):

    def test_empty(self):
        self.assertEqual(format_progress(0, 10, width=10), '[----------] 0/10 jobs (0%)')

    def test_half(self):
        self.assertEqual(format_progress(5, 10, width=10), '[#####-----] 5/10 jobs (50%)')

    def test_full(self):
        self.assertEqual(format_progress(10, 10, width=10), '[##########] 10/10 jobs (100%)')

    def test_done_is_clamped_to_total(self):
        # A transient over-count must never overflow the bar or the percentage.
        self.assertEqual(format_progress(12, 10, width=10), '[##########] 10/10 jobs (100%)')

    def test_bar_width_is_honored(self):
        line = format_progress(3, 4, width=20)
        bar = line[line.index('[') + 1:line.index(']')]
        self.assertEqual(len(bar), 20)
        self.assertEqual(bar.count('#'), 15)  # int(3/4 * 20)

    def test_failed_flag(self):
        self.assertTrue(format_progress(5, 10, width=10, failed=2).endswith(' - 2 failed'))
        self.assertNotIn('failed', format_progress(5, 10, width=10, failed=0))

    def test_elapsed_appended(self):
        self.assertIn(' 3s', format_progress(5, 10, width=10, elapsed=3.4))

    def test_extra_text_appended_after_percentage(self):
        line = format_progress(5, 10, width=10, extra='  5.0 MiB')
        self.assertEqual(line, '[#####-----] 5/10 jobs (50%)  5.0 MiB')

    def test_zero_total_does_not_crash(self):
        # No jobs to download: treated as 100% complete, no division by zero.
        self.assertEqual(format_progress(0, 0, width=10), '[##########] 0/0 jobs (100%)')


class TestProgressBar(SeqddTest):

    def test_inactive_on_non_tty(self):
        stream = io.StringIO()  # StringIO.isatty() is False
        bar = ProgressBar(10, stream=stream, width=10)
        self.assertFalse(bar.active)
        bar.update(5)
        bar.close(10)
        self.assertEqual(stream.getvalue(), '')

    def test_draws_on_tty(self):
        stream = _FakeTTY()
        bar = ProgressBar(10, stream=stream, width=10)
        self.assertTrue(bar.active)
        bar.update(2)
        out = stream.getvalue()
        self.assertTrue(out.startswith('\r'))
        self.assertIn('2/10 jobs', out)

    def test_close_terminates_line_and_is_idempotent(self):
        stream = _FakeTTY()
        bar = ProgressBar(10, stream=stream, width=10)
        bar.close(10, failed=1)
        first = stream.getvalue()
        self.assertTrue(first.endswith('\n'))
        self.assertIn('10/10 jobs', first)
        self.assertIn('1 failed', first)
        # A second close writes nothing more.
        bar.close(10)
        self.assertEqual(stream.getvalue(), first)

    def test_update_is_noop_after_close(self):
        stream = _FakeTTY()
        bar = ProgressBar(10, stream=stream, width=10)
        bar.close(10)
        after_close = stream.getvalue()
        bar.update(5)
        self.assertEqual(stream.getvalue(), after_close)

    def test_extra_text_is_drawn(self):
        stream = _FakeTTY()
        bar = ProgressBar(10, stream=stream, width=10)
        bar.update(2, extra='  3.0 MiB  (1 active)')
        self.assertIn('3.0 MiB', stream.getvalue())
        self.assertIn('(1 active)', stream.getvalue())


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

    def test_fill_reflects_bytes_not_jobs(self):
        # The whole point: a single job downloaded to 40% shows 40%, not 0/1 jobs.
        line = format_byte_progress(4 * 1024 * 1024, 10 * 1024 * 1024, width=10)
        self.assertEqual(line, '[####------] 4.0 MiB / 10.0 MiB (40%)')

    def test_overflow_is_clamped(self):
        self.assertEqual(format_byte_progress(150, 100, width=10), '[##########] 150 B / 100 B (100%)')

    def test_zero_total_is_empty(self):
        self.assertEqual(format_byte_progress(0, 0, width=10), '[----------] 0 B / 0 B (0%)')

    def test_suffix_and_elapsed(self):
        line = format_byte_progress(50, 100, width=10, suffix='2/4 jobs', elapsed=7.0)
        self.assertTrue(line.endswith('(50%)  2/4 jobs 7s'))


class TestProgressBarDrawFinish(SeqddTest):

    def test_draw_writes_line_in_place(self):
        stream = _FakeTTY()
        bar = ProgressBar(2, stream=stream, width=10)
        bar.draw('hello')
        self.assertEqual(stream.getvalue(), '\rhello\x1b[K')

    def test_finish_terminates_and_is_idempotent(self):
        stream = _FakeTTY()
        bar = ProgressBar(2, stream=stream, width=10)
        bar.finish('done')
        first = stream.getvalue()
        self.assertEqual(first, '\rdone\x1b[K\n')
        bar.finish('again')      # no-op after close
        bar.draw('again')        # no-op after close
        self.assertEqual(stream.getvalue(), first)

    def test_draw_is_noop_off_tty(self):
        stream = io.StringIO()
        bar = ProgressBar(2, stream=stream, width=10)
        bar.draw('hello')
        bar.finish('done')
        self.assertEqual(stream.getvalue(), '')
