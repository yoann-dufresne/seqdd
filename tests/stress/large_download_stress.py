"""
Standalone large-scale download stress tool for seqdd's pure-Python network layer.

It drives :mod:`seqdd.utils.net` against a local, controllable HTTP server (or a real URL) to
exercise, at scale, the situations a downloader must survive: big files, many parallel transfers,
repeated mid-stream connection drops absorbed within a single call, and resume across successive
calls (as when a user re-runs ``seqdd download``). Everything is generated in a temporary directory
and deleted at the end.

Run it from the repository root::

    python -m tests.stress.large_download_stress

Configuration (environment variables):

    SEQDD_BIG_DOWNLOAD_MB     size of each file in MiB (default 128)
    SEQDD_BIG_DOWNLOAD_FILES  number of parallel files in the scale phase (default 8)
    SEQDD_BIG_DOWNLOAD_DROPS  injected connection drops per file (default 3)
    SEQDD_BIG_DOWNLOAD_URL    download from this real URL instead of the local server
                              (only the scale/integrity phase runs in this mode)

Exits 0 if every phase passes, 1 otherwise.
"""

import logging
import os
import shutil
import sys
import tempfile
import time

from seqdd.errors import DownloadError
from seqdd.utils import net
from seqdd.utils.checksum import sha256sum
from seqdd.utils.scheduler import JobManager, FunctionJob
from tests.support.controllable_http_server import ControllableHTTPServer

_MiB = 1 << 20
SIZE = int(os.environ.get('SEQDD_BIG_DOWNLOAD_MB', '128')) * _MiB
FILES = int(os.environ.get('SEQDD_BIG_DOWNLOAD_FILES', '8'))
DROPS = int(os.environ.get('SEQDD_BIG_DOWNLOAD_DROPS', '3'))
REAL_URL = os.environ.get('SEQDD_BIG_DOWNLOAD_URL')


def _logger() -> logging.Logger:
    logger = logging.getLogger('seqdd')
    logger.setLevel(logging.WARNING)  # keep the scheduler quiet; this tool prints its own report
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())
    return logger


def _human(n: float) -> str:
    return f'{n / _MiB:.1f} MiB'


def build_source(path: str, size: int) -> str:
    """Write a ``size``-byte random file in chunks and return its sha256."""
    digest_bytes = 0
    with open(path, 'wb') as fh:
        remaining = size
        while remaining > 0:
            block = min(_MiB, remaining)
            fh.write(os.urandom(block))
            remaining -= block
            digest_bytes += block
    return sha256sum(path)


def run_parallel(url: str, dests: list[str], logger: logging.Logger, max_process: int = 4) -> JobManager:
    """Download ``url`` into every path of ``dests`` in parallel through the real JobManager."""
    log_dir = tempfile.mkdtemp(prefix='seqdd-stress-logs-')
    try:
        manager = JobManager(logger=logger, max_process=max_process, log_folder=log_dir)
        manager.start()
        for i, dest in enumerate(dests):
            manager.add_job(FunctionJob(func_to_run=net.download_file, func_args=(url, dest), name=f'dl_{i}'))
        while manager.remaining_jobs() > 0:
            time.sleep(0.1)
        manager.stop()
        manager.join()
        return manager
    finally:
        shutil.rmtree(log_dir, ignore_errors=True)


def phase_scale(server_url: str, expected_sha: str | None, workdir: str, logger: logging.Logger) -> bool:
    """Download FILES copies in parallel; check integrity and report aggregate throughput."""
    print(f'[scale] {FILES} parallel downloads of {_human(SIZE)} each ...')
    dests = [os.path.join(workdir, f'scale_{i}.bin') for i in range(FILES)]
    start = time.time()
    manager = run_parallel(server_url, dests, logger)
    elapsed = max(time.time() - start, 1e-9)

    ok = len(manager.completed_jobs) == FILES and not manager.failed_jobs
    shas = set()
    for dest in dests:
        if not os.path.isfile(dest) or os.path.getsize(dest) != SIZE:
            ok = False
            continue
        shas.add(sha256sum(dest))
    # All copies must be identical, and match the source when known.
    if len(shas) != 1:
        ok = False
    elif expected_sha is not None and expected_sha not in shas:
        ok = False

    total = FILES * SIZE
    print(f'[scale] {"OK" if ok else "FAIL"} — {_human(total)} in {elapsed:.1f}s '
          f'({total / _MiB / elapsed:.1f} MiB/s aggregate)')
    for dest in dests:
        if os.path.exists(dest):
            os.remove(dest)
    return ok


def phase_single_call_resume(server: ControllableHTTPServer, expected_sha: str, workdir: str) -> bool:
    """One download_file call must absorb DROPS mid-stream cuts and still complete."""
    print(f'[single-call resume] absorbing {DROPS} mid-stream drop(s) in one call ...')
    server.drops_remaining = DROPS
    server.drop_after = max(net._CHUNK_SIZE, SIZE // (DROPS + 2))
    server.reset_counters()
    dest = os.path.join(workdir, 'single_call.bin')
    try:
        # retries must allow at least DROPS in-call resumes.
        net.download_file(server.url(), dest, resume=True, retries=DROPS + 2)
    except DownloadError as err:
        print(f'[single-call resume] FAIL — {err}')
        return False
    ok = os.path.getsize(dest) == SIZE and sha256sum(dest) == expected_sha
    print(f'[single-call resume] {"OK" if ok else "FAIL"} — completed after '
          f'{server.get_count} request(s) ({server.get_count - 1} resume(s))')
    os.remove(dest)
    return ok


def phase_across_calls_resume(server: ControllableHTTPServer, expected_sha: str, workdir: str) -> bool:
    """Simulate re-running `seqdd download`: each call (retries=0) survives at most no drop,
    so it takes DROPS+1 calls to get through DROPS cuts, resuming each time."""
    print(f'[across-calls resume] re-running through {DROPS} drop(s), one cut per call ...')
    server.drops_remaining = DROPS
    server.drop_after = max(net._CHUNK_SIZE, SIZE // (DROPS + 2))
    server.reset_counters()
    dest = os.path.join(workdir, 'across_calls.bin')
    rounds = 0
    last_size = -1
    while True:
        rounds += 1
        try:
            net.download_file(server.url(), dest, resume=True, retries=0)
            break
        except DownloadError:
            size = os.path.getsize(dest) if os.path.exists(dest) else 0
            # Guard against a stuck loop (no progress) so the tool always terminates.
            if size <= last_size and rounds > DROPS + 2:
                print('[across-calls resume] FAIL — no progress, aborting')
                return False
            last_size = size
    ok = os.path.getsize(dest) == SIZE and sha256sum(dest) == expected_sha
    print(f'[across-calls resume] {"OK" if ok else "FAIL"} — completed in {rounds} call(s)')
    os.remove(dest)
    return ok


def main() -> int:
    logger = _logger()
    workdir = tempfile.mkdtemp(prefix='seqdd-stress-')
    results = []
    try:
        if REAL_URL:
            print(f'Stressing real URL: {REAL_URL}')
            print('(drop-injection phases are skipped for real servers)')
            results.append(('scale', phase_scale(REAL_URL, None, workdir, logger)))
        else:
            source = os.path.join(workdir, 'source.bin')
            print(f'Generating {_human(SIZE)} source file ...')
            expected_sha = build_source(source, SIZE)
            with ControllableHTTPServer(source) as server:
                results.append(('scale', phase_scale(server.url(), expected_sha, workdir, logger)))
                results.append(('single-call-resume',
                                phase_single_call_resume(server, expected_sha, workdir)))
                results.append(('across-calls-resume',
                                phase_across_calls_resume(server, expected_sha, workdir)))
            os.remove(source)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    print('\n=== summary ===')
    for name, ok in results:
        print(f'  {name:22s} {"PASS" if ok else "FAIL"}')
    passed = all(ok for _, ok in results)
    print(f'  {"ALL PASS" if passed else "FAILURES PRESENT"}')
    return 0 if passed else 1


if __name__ == '__main__':
    sys.exit(main())
