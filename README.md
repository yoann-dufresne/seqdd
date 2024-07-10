# SeqDD - The Sequence Data Downloader

A software to prepare a dataset environement from a set of accession number.
The goal is to make data environement more portable to allow better reproducibility.

Global command line:
```
    python3 -m seqdd [-h] [--register-location REGISTER_LOCATION] {init,add,download} ...
```

# Download data from an existing register

## From a register directory

If a register directory containing all needed dataset is present in your project (usually a .register folder), you can use it to ask SedDD to download everything for you.

# Construct your own sequence data register

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

