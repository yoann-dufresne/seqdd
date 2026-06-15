"""Provenance manifest (``seqdd-lock.json``).

The manifest records the size and SHA-256 of every file produced by a download so that the
data set can later be re-verified bit-for-bit. It is layout-agnostic: it stores paths relative
to the data directory, which works both for the ``<accession>/<files>`` layout (ENA, RefSeq,
sequences, assemblies) and for the flat ``url{idx}_<filename>`` layout (url, logan).
"""

import json
from datetime import datetime, timezone
from os import path, walk

from seqdd import __version__
from seqdd.utils.checksum import sha256sum

MANIFEST_NAME = 'seqdd-lock.json'


def _iter_files(datadir: str):
    """
    Yield (relative_path, absolute_path) for every file under datadir, except the manifest itself.

    :param datadir: The data directory to scan.
    """
    for root, _dirs, files in walk(datadir):
        for name in files:
            abs_path = path.join(root, name)
            rel_path = path.relpath(abs_path, datadir)
            if rel_path == MANIFEST_NAME:
                continue
            yield rel_path, abs_path


def build_manifest(datadir: str) -> dict:
    """
    Build the provenance manifest for everything currently present in datadir.

    :param datadir: The data directory to scan.
    :return: A manifest dict (seqdd version, UTC timestamp, and per-file size + sha256).
    """
    files = []
    for rel_path, abs_path in sorted(_iter_files(datadir)):
        files.append({
            'path': rel_path,
            'size': path.getsize(abs_path),
            'sha256': sha256sum(abs_path),
        })
    return {
        'seqdd_version': __version__,
        'created': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'files': files,
    }


def write_manifest(datadir: str) -> dict:
    """
    Build the manifest for datadir and write it to ``datadir/seqdd-lock.json``.

    :param datadir: The data directory to scan and where the manifest is written.
    :return: The manifest dict that was written.
    """
    manifest = build_manifest(datadir)
    with open(path.join(datadir, MANIFEST_NAME), 'w') as fw:
        json.dump(manifest, fw, indent=2, sort_keys=True)
        fw.write('\n')
    return manifest


def load_manifest(datadir: str) -> dict | None:
    """
    Load the manifest from datadir.

    :param datadir: The data directory containing the manifest.
    :return: The manifest dict, or None if the manifest file is absent.
    """
    manifest_path = path.join(datadir, MANIFEST_NAME)
    if not path.isfile(manifest_path):
        return None
    with open(manifest_path) as fr:
        return json.load(fr)


def load_manifest_file(manifest_path: str) -> dict:
    """
    Load a manifest from an explicit path (e.g. a lock file shared alongside a .reg file).

    :param manifest_path: Path to the manifest JSON file.
    :return: The manifest dict.
    :raises FileNotFoundError: If the manifest file does not exist.
    """
    if not path.isfile(manifest_path):
        raise FileNotFoundError(f'Manifest file not found: {manifest_path}')
    with open(manifest_path) as fr:
        return json.load(fr)


def verify_against(manifest: dict, datadir: str) -> dict:
    """
    Re-hash the files in datadir and compare them against a given manifest.

    :param manifest: A manifest dict (as produced by :func:`build_manifest`).
    :param datadir: The data directory to verify.
    :return: A dict with sorted lists of 'ok', 'corrupt', 'missing' and 'extra' relative paths.
    """
    result = {'ok': [], 'corrupt': [], 'missing': [], 'extra': []}
    recorded = {entry['path']: entry for entry in manifest.get('files', [])}

    for rel_path, entry in sorted(recorded.items()):
        abs_path = path.join(datadir, rel_path)
        if not path.isfile(abs_path):
            result['missing'].append(rel_path)
        elif sha256sum(abs_path) != entry['sha256']:
            result['corrupt'].append(rel_path)
        else:
            result['ok'].append(rel_path)

    on_disk = {rel for rel, _ in _iter_files(datadir)}
    result['extra'] = sorted(on_disk - set(recorded))

    return result


def verify_manifest(datadir: str) -> dict:
    """
    Re-hash the files in datadir and compare them against the manifest stored in datadir.

    :param datadir: The data directory to verify.
    :return: A dict with sorted lists of 'ok', 'corrupt', 'missing' and 'extra' relative paths.
    :raises FileNotFoundError: If no manifest is present in datadir.
    """
    manifest = load_manifest(datadir)
    if manifest is None:
        raise FileNotFoundError(f'No {MANIFEST_NAME} found in {datadir}')
    return verify_against(manifest, datadir)
