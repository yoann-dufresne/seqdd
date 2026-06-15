"""Checksums computed with the standard library (no external binary)."""

import hashlib

_CHUNK_SIZE = 1 << 20  # 1 MiB


def _file_digest(file_path: str, digest) -> str:
    """
    Update ``digest`` with the content of ``file_path`` (read in chunks) and return its hex digest.

    :param file_path: Path to the file to hash.
    :param digest: A ``hashlib`` digest object to feed.
    :return: The hexadecimal digest.
    """
    with open(file_path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(_CHUNK_SIZE), b''):
            digest.update(chunk)
    return digest.hexdigest()


def sha256sum(file_path: str) -> str:
    """
    Compute the SHA-256 hex digest of a file, reading it in chunks to bound memory usage.

    :param file_path: Path to the file to hash.
    :return: The hexadecimal SHA-256 digest.
    """
    return _file_digest(file_path, hashlib.sha256())


def md5sum(file_path: str) -> str:
    """
    Compute the MD5 hex digest of a file, reading it in chunks to bound memory usage.

    Replaces the former ``md5sum`` external binary used to validate ENA reads.

    :param file_path: Path to the file to hash.
    :return: The hexadecimal MD5 digest.
    """
    return _file_digest(file_path, hashlib.md5())
