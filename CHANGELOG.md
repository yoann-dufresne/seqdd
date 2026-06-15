# Changelog

All notable changes to SeqDD are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-06-15

### Added
- Provenance manifest `seqdd-lock.json`, written after every `download`, recording the size and
  SHA-256 of every produced file (layout-agnostic: works for both the `<accession>/` and the flat
  `url<idx>_<file>` layouts).
- `seqdd verify`: re-hashes downloaded data against the manifest (or an explicit one given with
  `-m`), reports `OK` / `CORRUPT` / `MISSING` / `EXTRA`, and exits non-zero on any problem.
- `seqdd status`: shows, per data type, which registered accessions are downloaded and which are
  missing.
- `seqdd export --with-lock`: exports the provenance manifest next to the `.reg` file
  (as `<register>.lock.json`) for verifiable redistribution.
- `seqdd download --dry-run`: previews the planned work without downloading anything.
- Startup preflight that checks the required external tools (`curl`, `gzip`, `md5sum`, `wget`)
  are available on the `PATH`, with a clear error otherwise.
- Network resilience: downloads now retry on transient failures and resume partial transfers
  (curl `--retry`/`-C -`, wget `--tries`/`-c`).

### Changed
- `seqdd download` now prints a final summary (succeeded / failed / canceled) and exits with a
  non-zero status if any job failed or was canceled — useful in pipelines and CI.
- Packaging: single-source version (`seqdd.__version__`), SPDX license metadata and keywords;
  fixed the author e-mail and the project description.

### Fixed
- Removed leftover debug prints emitted during `download`.
