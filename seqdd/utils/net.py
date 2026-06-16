"""
Pure-Python network layer for seqdd (no external binaries).

Every download and validation request goes through this module: HTTP(S) traffic is handled with
``requests`` (retries, redirections, resume via Range, TLS through ``certifi``) and ``ftp://`` URLs
are handled with the standard-library :mod:`ftplib`. This replaces the former ``curl``/``wget``
subprocess calls and makes seqdd work identically on Linux, macOS and Windows.

All public helpers are module-level functions so they stay picklable: they are run inside the
``multiprocessing`` subprocesses spawned by :class:`seqdd.utils.scheduler.FunctionJob`, which under
the ``spawn`` start method (Windows/macOS) requires importable, top-level callables.
"""

from __future__ import annotations

import gzip
import os
import shutil
import socket
from collections.abc import Callable
from ftplib import FTP, all_errors as ftp_errors
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .. import __version__
from ..errors import DownloadError

DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
_CHUNK_SIZE = 1 << 20  # 1 MiB
USER_AGENT = f'seqdd/{__version__} (+https://github.com/yoann-dufresne/seqdd)'

# Progress callback signature: progress(delta_bytes, total_bytes_or_None).
# ``delta_bytes`` is the size of the chunk just written; ``total`` is the expected size of the whole
# download when known (HTTP Content-Length / FTP SIZE), else None.
ProgressCallback = Callable[[int, 'int | None'], None]


def _normalize_url(url: str) -> str:
    """
    Ensure ``url`` carries an explicit scheme.

    ENA's ``fastq_ftp`` field returns schemeless paths such as ``ftp.sra.ebi.ac.uk/vol1/...``;
    ``curl`` used to add ``ftp://`` implicitly, but :mod:`requests`/:mod:`ftplib` will not. We
    default a schemeless URL to ``ftp://``.

    :param url: The URL to normalize.
    :return: The URL with an explicit scheme.
    """
    if '://' not in url:
        return f'ftp://{url}'
    return url


def _build_session(retries: int) -> requests.Session:
    """
    Build a :class:`requests.Session` that retries transient failures and connection refusals.

    :param retries: The number of retries on transient network errors.
    :return: A configured session with the seqdd User-Agent.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({'GET', 'HEAD'}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers['User-Agent'] = USER_AGENT
    return session


# --- HTTP(S) validation helpers -------------------------------------------------------------


def http_get(url: str, *, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES) -> bytes:
    """
    Fetch the body of ``url`` with a GET request.

    :param url: The URL to fetch (http/https).
    :param timeout: The per-request timeout in seconds.
    :param retries: The number of retries on transient network errors.
    :return: The response body as bytes.
    :raises DownloadError: If the request fails or the server answers with a status >= 400.
    """
    with _build_session(retries) as session:
        try:
            response = session.get(url, timeout=timeout, allow_redirects=True)
        except requests.RequestException as err:
            raise DownloadError(f'GET failed for {url}: {err}') from err
        if response.status_code >= 400:
            raise DownloadError(f'GET {url} returned HTTP {response.status_code}')
        return response.content


def http_get_text(url: str, *, encoding: str = 'utf-8', **kwargs) -> str:
    """
    Fetch the body of ``url`` and decode it as text.

    :param url: The URL to fetch (http/https).
    :param encoding: The encoding used to decode the body.
    :param kwargs: Extra keyword arguments forwarded to :func:`http_get`.
    :return: The decoded response body.
    :raises DownloadError: If the request fails or the server answers with a status >= 400.
    """
    return http_get(url, **kwargs).decode(encoding)


def http_status(url: str, *, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES) -> int:
    """
    Return the HTTP status code of ``url`` without downloading the body.

    A HEAD request is used; if the server rejects it (405/501, as some S3 endpoints do), it falls
    back to a tiny ranged GET. A 404 (or any other code) is returned as-is rather than raised, so
    callers can use it as an existence check.

    :param url: The URL to probe (http/https).
    :param timeout: The per-request timeout in seconds.
    :param retries: The number of retries on transient network errors.
    :return: The HTTP status code, or 0 if the host could not be reached.
    """
    with _build_session(retries) as session:
        try:
            response = session.head(url, timeout=timeout, allow_redirects=True)
            if response.status_code in (405, 501):
                response = session.get(
                    url, timeout=timeout, allow_redirects=True,
                    headers={'Range': 'bytes=0-0'}, stream=True,
                )
                response.close()
            return response.status_code
        except requests.RequestException:
            return 0


def http_head_headers(url: str, *, timeout: int = DEFAULT_TIMEOUT,
                       retries: int = DEFAULT_RETRIES) -> tuple[int, dict[str, str]]:
    """
    Return the status code and response headers of ``url`` (HEAD request).

    Used to inspect ``Content-Disposition`` when resolving a download filename.

    :param url: The URL to probe (http/https).
    :param timeout: The per-request timeout in seconds.
    :param retries: The number of retries on transient network errors.
    :return: A tuple ``(status_code, headers)``; status is 0 and headers empty if unreachable.
    """
    with _build_session(retries) as session:
        try:
            response = session.head(url, timeout=timeout, allow_redirects=True)
            return response.status_code, dict(response.headers)
        except requests.RequestException:
            return 0, {}


# --- Download helpers -----------------------------------------------------------------------


def download_file(url: str, dest: str, *, resume: bool = True, retries: int = DEFAULT_RETRIES,
                  timeout: int = DEFAULT_TIMEOUT, progress: ProgressCallback | None = None) -> None:
    """
    Download ``url`` to ``dest``, dispatching on the URL scheme (http/https vs ftp).

    HTTP(S) transfers stream the body in chunks and, when ``resume`` is set and a partial file
    already exists, ask the server for the missing byte range (appending on a 206, restarting on a
    200 because the server ignored the range).

    :param url: The URL to download. A schemeless URL is treated as ``ftp://``.
    :param dest: The destination file path.
    :param resume: Whether to resume a partially downloaded file.
    :param retries: The number of retries on transient network errors.
    :param timeout: The per-request timeout in seconds.
    :param progress: Optional callback invoked with the byte length of each freshly written chunk,
                     used to report live download progress; ``None`` disables reporting.
    :raises DownloadError: If the download fails after all retries.
    """
    url = _normalize_url(url)
    scheme = urlsplit(url).scheme
    if scheme == 'ftp':
        _download_ftp(url, dest, retries=retries, timeout=timeout, progress=progress)
    else:
        _download_http(url, dest, resume=resume, retries=retries, timeout=timeout, progress=progress)


def _content_total(response) -> int | None:
    """
    Best-effort total size of the resource from response headers.

    Uses ``Content-Range`` (``bytes start-end/total``) on a 206 partial response, otherwise
    ``Content-Length`` on a 200. Returns None when the size cannot be determined.

    :param response: The streamed :mod:`requests` response.
    :return: The full resource size in bytes, or None.
    """
    content_range = response.headers.get('Content-Range')
    if content_range and '/' in content_range:
        tail = content_range.rsplit('/', 1)[1].strip()
        if tail.isdigit():
            return int(tail)
    length = response.headers.get('Content-Length')
    if length and length.isdigit() and response.status_code != 206:
        return int(length)
    return None


def _download_http(url: str, dest: str, *, resume: bool, retries: int, timeout: int,
                   progress: ProgressCallback | None = None) -> None:
    """
    Download an http(s) URL to ``dest`` with streaming and Range-based resume.

    ``urllib3`` retries only cover establishing the response (connect/status), not a connection that
    drops *while* the body is streaming. To survive such mid-stream cuts within a single call, the
    streaming is wrapped in a bounded retry loop: on a network error during the body, a new request
    is issued whose ``Range`` resumes from the bytes already written. The loop is bounded by
    ``max(1, retries) + 1`` attempts so a server that always cuts at the same offset cannot loop
    forever.

    :param url: The http/https URL to download.
    :param dest: The destination file path.
    :param resume: Whether to resume a partially downloaded file.
    :param retries: The number of retries on transient network errors.
    :param timeout: The per-request timeout in seconds.
    :raises DownloadError: If the download fails after all attempts.
    """
    # ``retries`` is the number of retries beyond the first attempt (retries=0 -> a single attempt,
    # i.e. no in-call resume; the caller can still resume on a later call).
    max_attempts = max(0, retries) + 1
    last_err: Exception | None = None

    for _ in range(max_attempts):
        # Recompute the resume offset on every attempt: a previous attempt may have written part of
        # the body before being cut, and the next request must continue from there.
        existing = os.path.getsize(dest) if (resume and os.path.exists(dest)) else 0
        headers = {'Range': f'bytes={existing}-'} if existing else {}

        try:
            with _build_session(retries) as session:
                with session.get(url, timeout=timeout, allow_redirects=True,
                                 headers=headers, stream=True) as response:
                    if response.status_code == 416:
                        # Requested range not satisfiable: the file is already complete.
                        return
                    if response.status_code >= 400:
                        # A durable HTTP error (urllib3 already retried 5xx): do not loop on it.
                        raise DownloadError(f'GET {url} returned HTTP {response.status_code}')
                    # 206 => the server honored the range, append; otherwise (re)write from scratch.
                    mode = 'ab' if (existing and response.status_code == 206) else 'wb'
                    total = _content_total(response)
                    with open(dest, mode) as fh:
                        for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
                            if chunk:
                                fh.write(chunk)
                                if progress is not None:
                                    progress(len(chunk), total)
            return
        except requests.RequestException as err:
            # Mid-stream drop (or connection error): retry and resume, unless resume is disabled.
            last_err = err
            if not resume:
                break

    raise DownloadError(f'Download failed for {url}: {last_err}')


def download_and_gzip(url: str, dest: str, *, progress: ProgressCallback | None = None) -> None:
    """
    Download ``url`` to ``dest`` then gzip-compress it to ``dest + '.gz'``.

    Module-level wrapper used as a :class:`FunctionJob` target to replace the former
    ``curl | gzip`` job chain. The uncompressed intermediate file is removed afterwards.

    :param url: The URL to download.
    :param dest: The path of the uncompressed file; the result is written to ``dest + '.gz'``.
    :param progress: Optional callback invoked with the byte length of each downloaded chunk.
    :raises DownloadError: If the download fails.
    """
    download_file(url, dest, progress=progress)
    gz_path = f'{dest}.gz'
    with open(dest, 'rb') as src, gzip.open(gz_path, 'wb') as dst:
        shutil.copyfileobj(src, dst, length=_CHUNK_SIZE)
    os.remove(dest)


# --- FTP helpers ----------------------------------------------------------------------------


def _ftp_connect(host: str, timeout: int) -> FTP:
    """
    Open an anonymous FTP connection to ``host``.

    :param host: The FTP host name.
    :param timeout: The connection timeout in seconds.
    :return: A logged-in :class:`ftplib.FTP` connection.
    :raises DownloadError: If the connection or login fails.
    """
    try:
        ftp = FTP(host, timeout=timeout)
        ftp.login()
        return ftp
    except (ftp_errors, socket.error) as err:
        raise DownloadError(f'FTP connection to {host} failed: {err}') from err


def _ftp_size(ftp: FTP, remote_path: str) -> int | None:
    """
    Return the size of a remote file via the FTP ``SIZE`` command, or None if unavailable.

    :param ftp: An open, logged-in FTP connection.
    :param remote_path: The remote file path.
    :return: The file size in bytes, or None when the server does not support ``SIZE``.
    """
    try:
        ftp.voidcmd('TYPE I')
        return ftp.size(remote_path)
    except (ftp_errors, OSError):
        return None


def _writer_with_progress(fh, progress: ProgressCallback | None, total: int | None):
    """
    Build the ``ftplib`` ``retrbinary`` block callback, optionally reporting bytes.

    :param fh: The open destination file handle to write each block to.
    :param progress: Optional callback invoked with (block length, total size) for each block.
    :param total: The expected total size of the file, or None if unknown.
    :return: ``fh.write`` when no progress is requested, otherwise a wrapper that writes then reports.
    """
    if progress is None:
        return fh.write

    def _sink(chunk: bytes) -> None:
        fh.write(chunk)
        progress(len(chunk), total)

    return _sink


def _download_ftp(url: str, dest: str, *, retries: int, timeout: int,
                  progress: ProgressCallback | None = None) -> None:
    """
    Download a single file from an ``ftp://`` URL to ``dest``.

    :param url: The ftp URL to download.
    :param dest: The destination file path.
    :param retries: The number of attempts before giving up.
    :param timeout: The connection timeout in seconds.
    :raises DownloadError: If the download fails after all attempts.
    """
    parts = urlsplit(url)
    host = parts.hostname
    remote_path = parts.path

    last_err: Exception | None = None
    for _ in range(max(1, retries)):
        try:
            ftp = _ftp_connect(host, timeout)
            try:
                total = _ftp_size(ftp, remote_path)
                with open(dest, 'wb') as fh:
                    ftp.retrbinary(f'RETR {remote_path}', _writer_with_progress(fh, progress, total),
                                   blocksize=_CHUNK_SIZE)
            finally:
                _quietly_quit(ftp)
            return
        except (ftp_errors, socket.error, OSError) as err:
            last_err = err
    raise DownloadError(f'FTP download failed for {url}: {last_err}')


def download_ftp_dir(base_url: str, dest_dir: str, *, retries: int = DEFAULT_RETRIES,
                     timeout: int = DEFAULT_TIMEOUT,
                     progress: ProgressCallback | None = None) -> None:
    """
    Recursively download an FTP directory tree, mirroring ``wget -r -np -nH`` behavior.

    The leaf directory of ``base_url`` is recreated under ``dest_dir`` and the whole subtree is
    fetched into it (used for NCBI RefSeq assembly directories).

    :param base_url: The ftp URL of the directory to download (trailing slash tolerated).
    :param dest_dir: The local directory under which the tree is recreated.
    :param retries: The number of connection attempts before giving up.
    :param timeout: The connection timeout in seconds.
    :param progress: Optional callback invoked with the byte length of each downloaded block.
    :raises DownloadError: If the directory cannot be listed or downloaded.
    """
    base_url = _normalize_url(base_url)
    parts = urlsplit(base_url)
    host = parts.hostname
    remote_dir = parts.path.rstrip('/')
    leaf = os.path.basename(remote_dir)
    local_root = os.path.join(dest_dir, leaf)

    last_err: Exception | None = None
    for _ in range(max(1, retries)):
        try:
            ftp = _ftp_connect(host, timeout)
            try:
                _recursive_ftp_download(ftp, remote_dir, local_root, progress=progress)
            finally:
                _quietly_quit(ftp)
            return
        except (ftp_errors, socket.error, OSError) as err:
            last_err = err
    raise DownloadError(f'FTP directory download failed for {base_url}: {last_err}')


def _recursive_ftp_download(ftp: FTP, remote_dir: str, local_dir: str,
                            progress: ProgressCallback | None = None) -> None:
    """
    Walk ``remote_dir`` on an open FTP connection, downloading every file into ``local_dir``.

    :param ftp: An open, logged-in FTP connection.
    :param remote_dir: The remote directory path to walk.
    :param local_dir: The local directory that mirrors ``remote_dir``.
    :param progress: Optional callback invoked with the byte length of each downloaded block.
    """
    os.makedirs(local_dir, exist_ok=True)
    for name, kind in _ftp_list(ftp, remote_dir):
        remote_path = f'{remote_dir}/{name}'
        local_path = os.path.join(local_dir, name)
        if kind == 'dir':
            _recursive_ftp_download(ftp, remote_path, local_path, progress=progress)
        else:
            with open(local_path, 'wb') as fh:
                # The total size of a whole directory tree is not known up front, so dir downloads
                # report bytes with an unknown total (they appear in throughput, not in the byte bar).
                ftp.retrbinary(f'RETR {remote_path}', _writer_with_progress(fh, progress, None),
                               blocksize=_CHUNK_SIZE)


def _ftp_list(ftp: FTP, remote_dir: str) -> list[tuple[str, str]]:
    """
    List the entries of ``remote_dir``, classifying each as ``'dir'`` or ``'file'``.

    Uses ``MLSD`` when the server supports it; otherwise falls back to ``NLST`` plus a ``CWD``
    probe to tell directories from files.

    :param ftp: An open, logged-in FTP connection.
    :param remote_dir: The remote directory path to list.
    :return: A list of ``(name, kind)`` tuples (kind is ``'dir'`` or ``'file'``).
    """
    try:
        entries = []
        for name, facts in ftp.mlsd(remote_dir):
            if name in ('.', '..'):
                continue
            kind = 'dir' if facts.get('type') in ('dir', 'cdir', 'pdir') else 'file'
            entries.append((name, kind))
        return entries
    except ftp_errors:
        # Server without MLSD support: list names and probe each with CWD.
        names = ftp.nlst(remote_dir)
        entries = []
        for full in names:
            name = os.path.basename(full.rstrip('/'))
            if name in ('', '.', '..'):
                continue
            try:
                ftp.cwd(f'{remote_dir}/{name}')
                entries.append((name, 'dir'))
            except ftp_errors:
                entries.append((name, 'file'))
        return entries


def _quietly_quit(ftp: FTP) -> None:
    """
    Close an FTP connection, ignoring any error raised while quitting.

    :param ftp: The FTP connection to close.
    """
    try:
        ftp.quit()
    except ftp_errors:
        ftp.close()
