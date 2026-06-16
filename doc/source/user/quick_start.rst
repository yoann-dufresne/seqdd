.. SeqDD - Sequence Data Downloader
    Authors: Yoann Dufresne
    Copyright © 2024  Institut Pasteur (Paris), and CNRS.
    See the COPYRIGHT file for details
    SeqDD is distributed under the terms of the GNU General Public License (GPLv3).
    See the COPYING file for details.

.. _quick_start:


===========
Quick Start
===========

SeqDD is pure Python and runs on Linux, macOS and Windows. Its only third-party dependency is
``requests`` (installed automatically); no external command-line tool is required.

.. note::
   Adding accessions (``seqdd add``) validates them against the online sources (ENA, NCBI, S3),
   so it needs network access.

Usage examples
==============

Create a data register
----------------------

The default location ``.register`` is used to store the accessions. Accessions are grouped by
**data type**, selected with ``-t/--type``:

.. code-block:: shell

    seqdd init
    seqdd add -t assemblies -a GCA_000001635.9 GCA_003774525.2
    seqdd add -t readarchives -a SRR000001

The available data types are ``assemblies`` (GenBank ``GCA_`` assemblies), ``sequences``
(individual INSDC nucleotide records), ``refseq`` (NCBI RefSeq ``GCF_`` assemblies),
``readarchives`` (SRA/ENA/DRA raw reads), ``logan`` (Logan contigs/unitigs) and ``url``
(arbitrary URLs).

An alternative way to create the register is to load a ``.reg`` file during the init:

.. code-block:: shell

    seqdd init -r myregister.reg


Download data from an existing register
---------------------------------------

All the register files are downloaded into the data directory:

.. code-block:: shell

    seqdd download


Exporting my data register for others
-------------------------------------

.. code-block:: shell

    seqdd export -o myregister.reg



Tools description
=================

All the tools are applied to a register. Without specification, the ``.register`` directory is
used as a register. To specify a register directory, use the ``--register-location`` option.

.. code-block:: text

    usage: seqdd [-h] {init,add,download,export,list,remove,verify,status} ...

    Prepare a sequence dataset, download it and export .reg files for reproducibility.

    positional arguments:
      {init,add,download,export,list,remove,verify,status}
                            command to apply
        init                Initialise the data register
        add                 Add dataset(s) to manage
        download            Download data from the register. Pure Python: no
                            external command-line tool is required.
        export              Export the metadata into a .reg file. This file can be
                            loaded from other locations to download the exact same
                            data.
        list                List all the datasets from the register.
        remove              Remove dataset(s) from the register
        verify              Verify downloaded data against the provenance manifest
                            (seqdd-lock.json).
        status              Show which registered accessions are downloaded and
                            which are missing.

    options:
      -h, --help            show this help message and exit

    Reproducibility is crucial, let's try to improve it!


Init a dataset register
-----------------------

Subcommand init:
""""""""""""""""

.. code-block:: text

    usage: seqdd init [-h] [-f] [-r REGISTER_FILE] [--register-location REGISTER_LOCATION]

    options:
      -h, --help            show this help message and exit
      -f, --force           Force reconstruction of the register
      -r REGISTER_FILE, --register-file REGISTER_FILE
                            Init the local register from the register file

.. code-block:: shell

    seqdd init --register-file aregisterfile.reg


Add sequences to the register
-----------------------------

Subcommand add:
"""""""""""""""

.. code-block:: text

    usage: seqdd add [-h] -t {assemblies,logan,readarchives,refseq,sequences,url}
                     [-a ACCESSIONS [ACCESSIONS ...]] [-f FILE_OF_ACCESSIONS]
                     [--tmp-directory TMP_DIRECTORY] [--unitigs]
                     [--register-location REGISTER_LOCATION]

    options:
      -h, --help            show this help message and exit
      -t, --type {assemblies,logan,readarchives,refseq,sequences,url}
                            Downloadable data type.
      -a ACCESSIONS [ACCESSIONS ...], --accessions ACCESSIONS [ACCESSIONS ...]
                            List of accessions to register
      -f FILE_OF_ACCESSIONS, --file-of-accessions FILE_OF_ACCESSIONS
                            A file containing accessions to download, 1 per line
      --tmp-directory TMP_DIRECTORY
                            Temporary directory to store and organize the downloaded files
      --unitigs             Download unitigs instead of contigs for logan accessions.

.. code-block:: shell

    seqdd add -t assemblies -a GCA_000001635.9 GCA_003774525.2 -f accessions.txt


Download the dataset from an already setup register
---------------------------------------------------

Subcommand download
"""""""""""""""""""

.. code-block:: text

    usage: seqdd download [-h] [-d DOWNLOAD_DIRECTORY] [-p MAX_PROCESSES]
                          [-r REGISTER_FILE] [-f] [--tmp-directory TMP_DIRECTORY]
                          [--log-directory LOG_DIRECTORY] [--dry-run]
                          [--register-location REGISTER_LOCATION]

    options:
      -h, --help            show this help message and exit
      -d DOWNLOAD_DIRECTORY, --download-directory DOWNLOAD_DIRECTORY
                            Directory where all the data will be downloaded
      -p MAX_PROCESSES, --max-processes MAX_PROCESSES
                            Number of processes to run in parallel.
      -r REGISTER_FILE, --register-file REGISTER_FILE
                            Register file to import and download from.
      --dry-run             Show what would be downloaded without downloading anything.

.. code-block:: shell

    seqdd download --download-directory my_data


Export the dataset metadata to a .reg file
------------------------------------------

Subcommand export
"""""""""""""""""

.. code-block:: text

    usage: seqdd export [-h] [-o OUTPUT_REGISTER] [-d DOWNLOAD_DIRECTORY] [--with-lock]

    options:
      -h, --help            show this help message and exit
      -o OUTPUT_REGISTER, --output-register OUTPUT_REGISTER
                            Name of the register file. Please prefer filenames .reg terminated.
      --with-lock           Also export the provenance manifest as <register>.lock.json

.. code-block:: shell

    seqdd export --output-register myregister.reg


Verify downloaded data
----------------------

After a download, SeqDD writes a provenance manifest ``seqdd-lock.json`` recording the SHA-256 of
every file. ``verify`` re-hashes the data and exits with a non-zero status if anything is missing
or corrupted:

.. code-block:: shell

    seqdd verify -d my_data
