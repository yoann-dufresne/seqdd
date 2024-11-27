# SeqDD - The Sequence Data Downloader

A software to prepare a dataset environement from a set of accession number.
The goal is to make data environement more portable to allow better reproducibility.

# Install the tool

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
    seqdd add -s ncbi -a GCA_000001635.9 GCA_003774525.2
    seqdd add -s sra -a SRR000001
```

An altertnative way to create the register is to load a .reg file during the init as follow:

```bash
    seqdd init -r myregister.reg
```

## Download data from an existing register

All the register files are downloaded into the data directory

```bash
    seqdd download
```

## Exporting my data register for others

```bash
    seqdd export -o myregister.reg
```


# Tools description

All the tools are applied to a register. Without specification, the .register directory is used as a register. To specify a register directory, use the `--register-location` option before your command.

General command line:
```
    usage: seqdd [-h] [--register-location REGISTER_LOCATION] {init,add,download,export} ...

    Prepare a sequence dataset, download it and export .reg files for reproducibility.

    positional arguments:
    {init,add,download,export}
                            command to apply
        init                Initialise the data register
        add                 Add dataset(s) to manage
        download            Download data from the register. The download process needs sra-tools, ncbi command-line tools and wget.
        export              Export the metadata into a .reg file. This file can be loaded from other locations to download the exact same data.

    options:
    -h, --help            show this help message and exit
    --register-location REGISTER_LOCATION
                            Directory that store all info for the register

    Reproducibility is crutial, let's try to improve it!
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
    usage: seqdd add [-h] -s {ncbi,sra,url} [-a ACCESSIONS [ACCESSIONS ...]] [-f FILE_OF_ACCESSIONS]

    options:
    -h, --help            show this help message and exit
    -s {ncbi,sra,url}, --source {ncbi,sra,url}
                            Download source. Can download from ncbi genomes, sra or an arbitrary url (uses wget to download)
    -a ACCESSIONS [ACCESSIONS ...], --accessions ACCESSIONS [ACCESSIONS ...]
                            List of accessions to register
    -f FILE_OF_ACCESSIONS, --file-of-accessions FILE_OF_ACCESSIONS
                            A file containing accessions to download, 1 per line
```

Example with ncbi genome accessions
```bash
    seqdd add --sources_ko ncbi --accessions ACCESSION1 ACCESSION2 --file-of-accessions accessions.txt
```

## Download the dataset from an already setup register

Subcommand download
```
    usage: seqdd download [-h] [-d DOWNLOAD_DIRECTORY] [-p MAX_PROCESSES]

    options:
    -h, --help            show this help message and exit
    -d DOWNLOAD_DIRECTORY, --download-directory DOWNLOAD_DIRECTORY
                            Directory where all the data will be downloaded
    -p MAX_PROCESSES, --max-processes MAX_PROCESSES
                            Maximum number of processes to run in parallel.
```

```bash
    seqdd download --download-directory my_data
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
