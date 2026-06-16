.. SeqDD - Sequence Data Downloader
    Authors: Yoann Dufresne
    Copyright © 2024  Institut Pasteur (Paris), and CNRS.
    See the COPYRIGHT file for details
    SeqDD is distributed under the terms of the GNU General Public License (GPLv3).
    See the COPYING file for details.

.. _dev_overview:


=====================
Architecture overview
=====================

SeqDD is a layered, object-oriented and **pure-Python** command-line tool (its only third-party
dependency is ``requests``). It runs on Linux, macOS and Windows.

Register and data types
=======================

The central concept is the **register** (``.register/`` by default), a directory grouping
accessions by **data type**. Each data type is a :class:`~seqdd.register.data_type` container
(``assemblies``, ``sequences``, ``refseq``, ``readarchives``, ``logan``, ``url``) that knows which
accessions it manages and how to validate them, and delegates the *how to download* to a
**data source** (:mod:`seqdd.register.sources`: ``ENA``, ``RefSeq``, ``UrlServer``). Data types are
**auto-discovered**: dropping a new ``DataContainer`` subclass in ``register/data_type/`` with a
correctly annotated ``source`` parameter is enough (see
:doc:`datatype_manager <api/register/datatype_manager>`).

Network layer
=============

Every network access goes through the pure-Python :mod:`seqdd.utils.net` layer: ``requests`` for
HTTP(S) (with retries, redirections and ``Range``-based resume that survives mid-stream connection
drops) and the standard-library ``ftplib`` for ``ftp://`` URLs (including the recursive RefSeq
directory download). Compression and checksums use the ``gzip`` and ``hashlib`` standard-library
modules. No external command-line tool is used.

Jobs and parallelism
====================

Downloads are executed as a dependency graph of jobs (:mod:`seqdd.utils.scheduler`):
``FunctionJob`` runs a Python function in a subprocess, ``CmdLineJob`` runs a shell command, and
``JobManager`` schedules up to ``max_process`` ready jobs, cancelling the descendants of a failed
job. Subprocesses are started with a non-fork multiprocessing context (``forkserver``/``spawn``),
so job targets must be picklable, module-level callables. ``DownloadManager``
(:mod:`seqdd.utils.download`) builds the jobs per data type, interleaves them so several sources
download in parallel, and writes a provenance manifest (``seqdd-lock.json``,
:mod:`seqdd.utils.manifest`) recording the SHA-256 of every downloaded file.
