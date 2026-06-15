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
                  timeout: int = DEFAULT_TIMEOUT) -> None:
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
    :raises DownloadError: If the download fails after all retries.
    """
    url = _normalize_url(url)
    scheme = urlsplit(url).scheme
    if scheme == 'ftp':
        _download_ftp(url, dest, retries=retries, timeout=timeout)
    else:
        _download_http(url, dest, resume=resume, retries=retries, timeout=timeout)


def _download_http(url: str, dest: str, *, resume: bool, retries: int, timeout: int) -> None:
    """
    Download an http(s) URL to ``dest`` with streaming and optional Range-based resume.

    :param url: The http/https URL to download.
    :param dest: The destination file path.
    :param resume: Whether to resume a partially downloaded file.
    :param retries: The number of retries on transient network errors.
    :param timeout: The per-request timeout in seconds.
    :raises DownloadError: If the download fails.
    """
    headers = {}
    existing = os.path.getsize(dest) if (resume and os.path.exists(dest)) else 0
    if existing:
        headers['Range'] = f'bytes={existing}-'

    with _build_session(retries) as session:
        try:
            with session.get(url, timeout=timeout, allow_redirects=True,
                             headers=headers, stream=True) as response:
                if response.status_code == 416:
                    # Requested range not satisfiable: the file is already complete.
                    return
                if response.status_code >= 400:
                    raise DownloadError(f'GET {url} returned HTTP {response.status_code}')
                # 206 => the server honored the range, append; otherwise (re)write from scratch.
                mode = 'ab' if (existing and response.status_code == 206) else 'wb'
                with open(dest, mode) as fh:
                    for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
                        if chunk:
                            fh.write(chunk)
        except requests.RequestException as err:
            raise DownloadError(f'Download failed for {url}: {err}') from err


def download_and_gzip(url: str, dest: str) -> None:
    """
    Download ``url`` to ``dest`` then gzip-compress it to ``dest + '.gz'``.

    Module-level wrapper used as a :class:`FunctionJob` target to replace the former
    ``curl | gzip`` job chain. The uncompressed intermediate file is removed afterwards.

    :param url: The URL to download.
    :param dest: The path of the uncompressed file; the result is written to ``dest + '.gz'``.
    :raises DownloadError: If the download fails.
    """
    download_file(url, dest)
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


def _download_ftp(url: str, dest: str, *, retries: int, timeout: int) -> None:
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
                with open(dest, 'wb') as fh:
                    ftp.retrbinary(f'RETR {remote_path}', fh.write, blocksize=_CHUNK_SIZE)
            finally:
                _quietly_quit(ftp)
            return
        except (ftp_errors, socket.error, OSError) as err:
            last_err = err
    raise DownloadError(f'FTP download failed for {url}: {last_err}')


def download_ftp_dir(base_url: str, dest_dir: str, *, retries: int = DEFAULT_RETRIES,
                     timeout: int = DEFAULT_TIMEOUT) -> None:
    """
    Recursively download an FTP directory tree, mirroring ``wget -r -np -nH`` behavior.

    The leaf directory of ``base_url`` is recreated under ``dest_dir`` and the whole subtree is
    fetched into it (used for NCBI RefSeq assembly directories).

    :param base_url: The ftp URL of the directory to download (trailing slash tolerated).
    :param dest_dir: The local directory under which the tree is recreated.
    :param retries: The number of connection attempts before giving up.
    :param timeout: The connection timeout in seconds.
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
                _recursive_ftp_download(ftp, remote_dir, local_root)
            finally:
                _quietly_quit(ftp)
            return
        except (ftp_errors, socket.error, OSError) as err:
            last_err = err
    raise DownloadError(f'FTP directory download failed for {base_url}: {last_err}')


def _recursive_ftp_download(ftp: FTP, remote_dir: str, local_dir: str) -> None:
    """
    Walk ``remote_dir`` on an open FTP connection, downloading every file into ``local_dir``.

    :param ftp: An open, logged-in FTP connection.
    :param remote_dir: The remote directory path to walk.
    :param local_dir: The local directory that mirrors ``remote_dir``.
    """
    os.makedirs(local_dir, exist_ok=True)
    for name, kind in _ftp_list(ftp, remote_dir):
        remote_path = f'{remote_dir}/{name}'
        local_path = os.path.join(local_dir, name)
        if kind == 'dir':
            _recursive_ftp_download(ftp, remote_path, local_path)
        else:
            with open(local_path, 'wb') as fh:
                ftp.retrbinary(f'RETR {remote_path}', fh.write, blocksize=_CHUNK_SIZE)


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
