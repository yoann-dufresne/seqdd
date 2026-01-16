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
    seqdd add -t assembly -a GCA_000001635.9 GCA_003774525.2
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


# Tools description

All the tools are applied to a register. Without specification, the .register directory is used as a register.
To specify a register directory, use the `--register-location` option before your command.

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
    usage: seqdd add [-h] -t {assemblies,logan,readarchives,url} [-a ACCESSIONS [ACCESSIONS ...]] [-f FILE_OF_ACCESSIONS] [--tmp-directory TMP_DIRECTORY] [--unitigs] [--register-location REGISTER_LOCATION]

    options:
        -h, --help            show this help message and exit
        -t {assemblies,logan,readarchives,url}, --type {assemblies,logan,readarchives,url}
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

## Remove accessions from the register

Subcommand remove:
```
    usage: seqdd remove [-h] [-t {assemblies,logan,readarchives,url}] [-a ACCESSIONS [ACCESSIONS ...]] [--register-location REGISTER_LOCATION]

    options:
        -h, --help            show this help message and exit
        -t {assemblies,logan,readarchives,url}, --type {assemblies,logan,readarchives,url}
                                Delete only from the given type. If not specified, removed from all the types. (default: None)
        -a ACCESSIONS [ACCESSIONS ...], --accessions ACCESSIONS [ACCESSIONS ...]
                                List of accessions to remove from the register. Each accession can be a regular expression. (default: None)
        --register-location REGISTER_LOCATION
                                Directory that store all info for the register (default: .register)
```

## List the content of the register

Subcommand list:
```
    usage: seqdd list [-h] [-t {assemblies,logan,readarchives,url}] [-r REGULAR_EXPRESSIONS [REGULAR_EXPRESSIONS ...]]
                  [--register-location REGISTER_LOCATION]

    options:
        -h, --help            show this help message and exit
        -t {assemblies,logan,readarchives,url}, --type {assemblies,logan,readarchives,url}
                                List only the datasets from the given type. If not specified, list all the datasets. (default: None)
        -r REGULAR_EXPRESSIONS [REGULAR_EXPRESSIONS ...], --regular-expressions REGULAR_EXPRESSIONS [REGULAR_EXPRESSIONS ...]
                                List only the datasets accessions that match at least one of the given regular expressions (default: [''])
        --register-location REGISTER_LOCATION
                                Directory that store all info for the register (default: .register)
```
