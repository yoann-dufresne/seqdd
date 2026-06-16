# SeqDD – Sequence Data Downloader

Rebuild and share a biological sequence dataset from a plain list of accession numbers — in one command.

## What it does

Reproducing the dataset of a bioinformatics paper is often the painful part: the supplementary
materials list dozens or hundreds of sequences, sometimes as direct links, sometimes as bare
accession numbers, sometimes with no clear instructions.

SeqDD turns that list into a reproducible dataset. You collect accessions into a **register**,
download them all with one command, and export the register as a small portable `.reg` text file
that anyone can re-import to get **exactly the same data**.

Typical uses:

- **Distribute a dataset** — e.g. a curated list of NCBI accessions for all species of a taxon.
- **Benchmark tools** on exactly the same input data.
- **Ship example datasets** with a pipeline, ready to download.

## Requirements

- Python ≥ 3.10
- Linux, macOS or Windows
- Internet access for `add` and `download` (accessions are validated and fetched online)

The only third-party dependency, [`requests`](https://pypi.org/project/requests/), is installed
automatically.

## Installation

```bash
git clone https://github.com/yoann-dufresne/seqdd.git
cd seqdd
pip install .
```

## Quick start

```bash
# 1. Create a register (a .register/ directory in the current folder)
seqdd init

# 2. Add accessions, grouped by data type (-t). This validates them online.
seqdd add -t assemblies   -a GCA_000001635.9
seqdd add -t readarchives -a SRR000001

# 3. Download everything into data/
seqdd download

# 4. Check what is downloaded, and verify integrity
seqdd status
seqdd verify -d data
```

Downloaded files land in `data/`, and the register lives in `.register/`. If you run `add` before
`init`, SeqDD creates the register automatically.

## Supported data types

Choose the data type with `-t/--type` when you `add` accessions:

| `-t` | Data | Example accession | Source |
|------|------|-------------------|--------|
| `assemblies` | GenBank genome assemblies | `GCA_000001635.9` | ENA |
| `sequences` | Individual nucleotide records (GenBank/EMBL/ENA) | `U00096.3`, `MN908947` | ENA |
| `refseq` | NCBI RefSeq genome assemblies | `GCF_000001635.9` | NCBI |
| `readarchives` | Raw sequencing reads (SRA/ENA/DRA) | `SRR000001`, `ERR…` | ENA |
| `logan` | Logan contigs/unitigs (assembled SRA) | `SRR000001` | Logan (S3) |
| `url` | Any file at a direct URL | `https://…/file.fa.gz` | the URL |

Accessions that can't be reached are skipped with a warning, so a typo never silently corrupts your
dataset.

## The register and `.reg` files

The **register** is a small directory (`.register/` by default) that records the accessions you want,
grouped by data type. It is the heart of SeqDD:

- `seqdd export -o mydataset.reg` writes the whole register to a single portable `.reg` text file.
- A colleague runs `seqdd download -r mydataset.reg` to rebuild the very same dataset.

For bit-for-bit reproducibility, you can also ship a lock file (see
[Reproducible redistribution](#reproducible-redistribution)).

## Commands

| Command | What it does |
|---------|--------------|
| `seqdd init` | Create an empty register (`-r file.reg` to seed it from a `.reg`). |
| `seqdd add` | Add accessions of a given `-t` type (`-a acc…` and/or `-f file`). |
| `seqdd download` | Download the registered data into `-d data/`. |
| `seqdd list` | List the registered accessions. |
| `seqdd status` | Show which accessions are downloaded and which are missing. |
| `seqdd remove` | Remove accessions from the register. |
| `seqdd export` | Write the register to a portable `.reg` file. |
| `seqdd verify` | Re-hash downloaded data against the provenance manifest. |

Run `seqdd <command> -h` for the full list of options. A few common examples:

```bash
# Add accessions from a file (one per line), or several sequence records
seqdd add -t readarchives -f accessions.txt
seqdd add -t sequences -a U00096.3 MN908947

# Preview a download without fetching anything
seqdd download --dry-run

# Download into a specific directory
seqdd download -d my_data

# Export the register to share it
seqdd export -o mydataset.reg
```

`download` writes a provenance manifest (`seqdd-lock.json`) in the download directory when it
finishes, and exits with a non-zero status if any file fails to download.

## Reproducible redistribution

To let someone reproduce your dataset *exactly*, ship the `.reg` together with its lock file:

```bash
# Producer
seqdd download -d data
seqdd export -o mydataset.reg -d data --with-lock   # writes mydataset.reg + mydataset.lock.json

# Consumer
seqdd download -r mydataset.reg -d data
seqdd verify -d data -m mydataset.lock.json         # confirms a bit-for-bit identical dataset
```

## Testing and development

```bash
pip install .[dev]                  # adds coverage, ruff, Sphinx
coverage run --source=seqdd         # = python3 -m unittest discover -s tests
coverage report
ruff check ./seqdd ./tests
```

Network-dependent functional tests live in `tests/functional/` (`./tests/functional/main.sh`). The
developer documentation under `doc/` covers the architecture and the large-scale download / stress
tests.
