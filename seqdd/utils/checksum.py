"""SHA-256 checksums computed with the standard library (no external binary)."""

import hashlib

_CHUNK_SIZE = 1 << 20  # 1 MiB


def sha256sum(file_path: str) -> str:
    """
    Compute the SHA-256 hex digest of a file, reading it in chunks to bound memory usage.

    :param file_path: Path to the file to hash.
    :return: The hexadecimal SHA-256 digest.
    """
    digest = hashlib.sha256()
    with open(file_path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(_CHUNK_SIZE), b''):
            digest.update(chunk)
    return digest.hexdigest()
