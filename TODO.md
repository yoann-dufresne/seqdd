# Roadmap / TODO

> Short list of known follow-ups. Done items live in `CHANGELOG.md`.

## Distribution (release)
* Publish to PyPI (`pip install seqdd`).
* Bioconda recipe.
* Docker / Apptainer image bundling `curl`, `gzip`, `coreutils`, `wget`.
* Zenodo DOI (wired through `CITATION.cff`).

## Features
* Rate limiting expressed as queries-per-second instead of a fixed delay.
* Richer data: assembly companion files (GFF3 annotation, proteins), ENA metadata export,
  `add` from an ENA Portal query.
* Google Cloud Storage (`gs://`) URL support.

## Documentation
* Finish the Sphinx API reference (utils: binaries / checksum / manifest / commands).
* User guide section on verifiable reproducibility (lock file + `verify`).
