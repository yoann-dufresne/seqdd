# SeqDD - The Sequence Data Downloader

A software to prepare a dataset environement from a set of accession number.
The goal is to make data environement more portable to allow better reproducibility.


# Usage examples

## Create a data register

These commands will create a register of 3 datasets. The default location .register is used to store the accessions.

```bash
    python3 -m seqdd init
    python3 -m seqdd add -s ncbi -a GCA_000001635 GCA_003774525.2
    python3 -m seqdd add -s sra -a SRR000001
```

## Download data from an existing register

All the register files are downloaded into the data directory

```bash
    python3 -m seqdd dowload
```


# Tools description

All the tools are applied to a register. Without specification, the .register directory is used as a register. To specify a register directory, use the `--register-location` option before your command.

General command line:
```bash
    python3 -m seqdd [-h] [--register-location REGISTER_LOCATION] {init,add,download} [options]
```

## Init a a dataset register

Subcommand init:
```
    usage: seqdd init [-h] [-f]

    options:
      -h, --help   show this help message and exit
      -f, --force  Force reconstruction of the register
```

```bash
    python3 -m seqdd init
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
    python3 -m seqdd add --sources ncbi --accessions ACCESSION1 ACCESSION2 --file-of-accessions accessions.txt
```

## Download the dataset from a register

Subcommand download
```
    usage: seqdd download [-h] [-d DOWNLOAD_DIRECTORY]

    options:
      -h, --help            show this help message and exit
      -d DOWNLOAD_DIRECTORY, --download-directory DOWNLOAD_DIRECTORY
                            Directory where all the data will be downloaded
```

```bash
    python3 -m seqdd download --download-directory my_data
```
