# SeqDD – Sequence Data Downloader

SeqDD is a tool designed to prepare reproducible dataset environments from a list of sequence accession numbers.  
Its primary goal is to make data handling more portable and reproducible by automating dataset retrieval and organization.

## Why use SeqDD?

Imagine you are reading a bioinformatics paper describing a new sequence analysis tool.  
You’re impressed and decide to try it yourself.

The software itself is easy to download and compile — great.  
But then comes the hard part: reproducing the results. The paper references dozens (or hundreds) of sequence files listed in the supplementary materials. Sometimes you’re given direct links; sometimes only accession numbers; sometimes no clear instructions at all.

This is exactly where **SeqDD** helps.

SeqDD simplifies data reproducibility by allowing you to download and recreate complete sequence datasets with a single command, directly from a curated list of accession numbers.

## When should I use SeqDD?

Use SeqDD whenever you need to download or redistribute the same set of sequences multiple times.  
Typical use cases include:

- **Dataset distribution**  
  For example, if you are a Foraminifera specialist and have compiled a list of NCBI accessions representing all known species of a given class.

- **Comparative software benchmarks**  
  Ensuring all tools are tested on exactly the same input data.

- **Example datasets for software or pipelines**  
  Providing users with a ready-to-use, reproducible dataset.

In all these cases, creating a local registry and exporting it for your users greatly simplifies data access and improves reproducibility.


# Install the tool

SeqDD is a pure-Python tool. It runs on **Linux, macOS and Windows** and its only third-party
dependency is [`requests`](https://pypi.org/project/requests/) (installed automatically). No
external command-line tool (`curl`, `wget`, `gzip`, `md5sum`, …) is required anymore.

> Note: adding accessions (`seqdd add`) validates them against the online sources (ENA, NCBI, S3),
> so it needs network access.

## From the sources

```bash
    git clone https://github.com/yoann-dufresne/seqdd.git seqdd/
    cd seqdd
    pip install .
```

# Usage examples

## Create a data register

Creates a register of 3 datasets. The default location .register is used to store the accessions.

```bash
    seqdd init
    # If the init is ignored, seqdd will automatically create it inside of the current directory
    seqdd add -t assemblies -a GCA_000001635.9 GCA_003774525.2
    seqdd add -t readarchives -a SRR000001
```

An altertnative way to create the register is to load a .reg file during the init as follow:

```bash
    seqdd init -r myregister.reg
```

## Download data from an existing register

All the register files are downloaded into the data directory

```bash
    # From the directory where the register is initialized
    seqdd download
```

## Exporting my register

```bash
    seqdd export -o myregister.reg
```

> Looking to run the test suite? See [Testing and development](#testing-and-development) at the end.


# Tools description

All the tools are applied to a register. Without specification, the .register directory is used as a register.
To specify a register directory, use the `--register-location` option before your command.

General command line:
```
    usage: seqdd [-h] {init,add,download,export,list,remove,verify,status} ...

    Prepare a sequence dataset, download it and export .reg files for reproducibility.

    positional arguments:
    {init,add,download,export,list,remove,verify,status}
                            command to apply
        init                Initialise the data register
        add                 Add dataset(s) to manage
        download            Download data from the register. Pure Python: no external command-line tool is required.
        export              Export the metadata into a .reg file. This file can be loaded from other locations to download the exact same data.
        list                List all the datasets from the register.
        remove              Remove dataset(s) from the register
        verify              Verify downloaded data against the provenance manifest (seqdd-lock.json).
        status              Show which registered accessions are downloaded and which are missing.

    options:
    -h, --help            show this help message and exit

    Reproducibility is crucial, let's try to improve it!
```

## Init a a dataset register

Subcommand init:
```
    usage: seqdd init [-h] [-f] [-r REGISTER_FILE]

    options:
    -h, --help            show this help message and exit
    -f, --force           Force reconstruction of the register
    -r REGISTER_FILE, --register-file REGISTER_FILE
                            Init the local register from the register file
```

```bash
    seqdd init --register-file aregisterfile.reg
```

## Add sequences to the register

Subcommand add:
```
    usage: seqdd add [-h] -t {assemblies,logan,readarchives,refseq,sequences,url} [-a ACCESSIONS [ACCESSIONS ...]] [-f FILE_OF_ACCESSIONS] [--tmp-directory TMP_DIRECTORY] [--unitigs] [--register-location REGISTER_LOCATION]

    options:
        -h, --help            show this help message and exit
        -t {assemblies,logan,readarchives,refseq,sequences,url}, --type {assemblies,logan,readarchives,refseq,sequences,url}
                                Downloadable data type. (default: None)
        -a ACCESSIONS [ACCESSIONS ...], --accessions ACCESSIONS [ACCESSIONS ...]
                                List of accessions to register (default: [])
        -f FILE_OF_ACCESSIONS, --file-of-accessions FILE_OF_ACCESSIONS
                                A file containing accessions to download, 1 per line (default: )
        --tmp-directory TMP_DIRECTORY
                                Temporary directory to store and organize the downloaded files (default: /tmp/seqdd)
        --unitigs             Download unitigs instead of contigs for logan accessions. (default: False)
        --register-location REGISTER_LOCATION
                                Directory that store all info for the register (default: .register)
```

Example with assembly (GenBank GCA) and read archive accessions:
```bash
    seqdd add --type assemblies --accessions GCA_000001635.9 GCA_003774525.2
    seqdd add --type readarchives --file-of-accessions accessions.txt
    seqdd add --type sequences --accessions U00096.3 MN908947
```

> Note: `add` validates every accession against its online source (ENA API for
> assemblies and read archives, Logan S3 for logan), so it requires internet
> access. Unreachable accessions are skipped with a warning.

## Download the dataset from an already setup register

Subcommand download
```
    usage: seqdd download [-h] [-d DOWNLOAD_DIRECTORY] [-p MAX_PROCESSES] [-r REGISTER_FILE] [-f]
                          [--tmp-directory TMP_DIRECTORY] [--log-directory LOG_DIRECTORY] [--dry-run]
                          [--register-location REGISTER_LOCATION]

    options:
    -h, --help            show this help message and exit
    -d DOWNLOAD_DIRECTORY, --download-directory DOWNLOAD_DIRECTORY
                            Directory where all the data will be downloaded (default: data)
    -p MAX_PROCESSES, --max-processes MAX_PROCESSES
                            Number of processes to run in parallel.
    -r REGISTER_FILE, --register-file REGISTER_FILE
                            Register file to import and download from.
    -f, --force           Used only with --register-file. Force reconstruction of the local register.
    --tmp-directory TMP_DIRECTORY
                            Temporary directory to store and organize the downloaded files
    --log-directory LOG_DIRECTORY
                            Directory where all the logs will be stored (default: logs)
    --dry-run             Show what would be downloaded without downloading anything.
```

`download` exits with a non-zero status if any download fails or is canceled, and writes a
provenance manifest (`seqdd-lock.json`) in the download directory once finished.

```bash
    seqdd download --download-directory my_data
    seqdd download --dry-run            # preview without downloading
```

## Export the dataset metadata to a .reg file

Subcommand export
```
    usage: seqdd export [-h] [-o OUTPUT_REGISTER]

    options:
    -h, --help            show this help message and exit
    -o OUTPUT_REGISTER, --output-register OUTPUT_REGISTER
                            Name of the register file. Please prefer filenames .reg terminated.
```

```bash
    seqdd export --output-register myregister.reg
```

## Remove accessions from the register

Subcommand remove:
```
    usage: seqdd remove [-h] [-t {assemblies,logan,readarchives,refseq,sequences,url}] [-a ACCESSIONS [ACCESSIONS ...]] [--register-location REGISTER_LOCATION]

    options:
        -h, --help            show this help message and exit
        -t {assemblies,logan,readarchives,refseq,sequences,url}, --type {assemblies,logan,readarchives,refseq,sequences,url}
                                Delete only from the given type. If not specified, removed from all the types. (default: None)
        -a ACCESSIONS [ACCESSIONS ...], --accessions ACCESSIONS [ACCESSIONS ...]
                                List of accessions to remove from the register. Each accession can be a regular expression. (default: None)
        --register-location REGISTER_LOCATION
                                Directory that store all info for the register (default: .register)
```

## List the content of the register

Subcommand list:
```
    usage: seqdd list [-h] [-t {assemblies,logan,readarchives,refseq,sequences,url}] [-r REGULAR_EXPRESSIONS [REGULAR_EXPRESSIONS ...]]
                  [--register-location REGISTER_LOCATION]

    options:
        -h, --help            show this help message and exit
        -t {assemblies,logan,readarchives,refseq,sequences,url}, --type {assemblies,logan,readarchives,refseq,sequences,url}
                                List only the datasets from the given type. If not specified, list all the datasets. (default: None)
        -r REGULAR_EXPRESSIONS [REGULAR_EXPRESSIONS ...], --regular-expressions REGULAR_EXPRESSIONS [REGULAR_EXPRESSIONS ...]
                                List only the datasets accessions that match at least one of the given regular expressions (default: [''])
        --register-location REGISTER_LOCATION
                                Directory that store all info for the register (default: .register)
```

## Check the download status

Show, per data type, which registered accessions are already downloaded and which are missing:

```bash
    seqdd status                       # uses the default data/ directory
    seqdd status -t assemblies -d my_data
```

## Verify downloaded data

After a download, SeqDD writes a provenance manifest `seqdd-lock.json` in the download directory,
recording the SHA-256 of every file. `verify` re-hashes the data and reports any missing or
corrupted file, exiting with a non-zero status if so:

```bash
    seqdd verify -d my_data
```

## Reproducible redistribution (lock file)

To let someone reproduce *exactly* your dataset, ship the `.reg` together with the lock file:

```bash
    # Producer
    seqdd download -d data
    seqdd export -o mydataset.reg -d data --with-lock   # writes mydataset.reg + mydataset.lock.json

    # Consumer
    seqdd download -r mydataset.reg -d data
    seqdd verify -d data -m mydataset.lock.json         # confirms a bit-for-bit identical dataset
```


# Testing and development

Install the development extras (adds `coverage`, `ruff`, and Sphinx):

```bash
    pip install .[dev]
```

## Unit tests and coverage

The unit tests are pure Python and run on **Linux, macOS and Windows without any network access** —
downloads are exercised against a local, in-process HTTP server:

```bash
    coverage run --source=seqdd      # equivalent to: python3 -m unittest discover -s tests
    coverage report
```

Run a single module, for example:

```bash
    python3 -m unittest tests.test_download_large -v
```

## Lint and format

```bash
    ruff check ./seqdd ./tests
    ruff format ./seqdd ./tests
```

## Functional tests (network)

`tests/functional/*.sh` drive the real `seqdd` CLI against live servers (ENA/EBI, NCBI, S3). They
need network access and a POSIX shell:

```bash
    ./tests/functional/main.sh
```

## Large-scale download tests (interruption & resume)

`tests/test_download_large.py` covers, deterministically and offline, the situations a downloader
must survive: integrity of relatively big files, **resume after a mid-stream connection drop**,
servers honoring/ignoring HTTP `Range`, transient `503`, parallel downloads through the scheduler,
and the full `add -t url` + `download` pipeline with **resume across runs**. Default file sizes are
modest for CI; scale them up with environment variables:

```bash
    # 64 MiB files, 6 parallel downloads in the parallel scenario
    SEQDD_TEST_DOWNLOAD_MB=64 SEQDD_TEST_DOWNLOAD_FILES=6 \
        python3 -m unittest tests.test_download_large -v
```

For a heavier, manual stress run (big files, many parallel transfers, repeated injected connection
drops), use the standalone tool:

```bash
    python3 -m tests.stress.large_download_stress

    # bigger run: 8 parallel files of 256 MiB, 5 injected drops each
    SEQDD_BIG_DOWNLOAD_MB=256 SEQDD_BIG_DOWNLOAD_FILES=8 SEQDD_BIG_DOWNLOAD_DROPS=5 \
        python3 -m tests.stress.large_download_stress

    # stress a real URL instead of the local server (scale/integrity phase only)
    SEQDD_BIG_DOWNLOAD_URL=https://example.org/big.file \
        python3 -m tests.stress.large_download_stress
```

It prints a per-phase report and exits non-zero if any check fails. Every artifact is written under a
temporary directory and removed at the end.

