.. SeqDD - Sequence Data Downloader
    Authors: Yoann Dufresne
    Copyright © 2024  Institut Pasteur (Paris), and CNRS.
    See the COPYRIGHT file for details
    SeqDD is distributed under the terms of the GNU General Public License (GPLv3).
    See the COPYING file for details.


net API reference
=================

Pure-Python network layer: every download and validation request goes through this module.
HTTP(S) traffic uses ``requests`` (retries, redirections, resume via ``Range``) and ``ftp://``
URLs use the standard-library :mod:`ftplib`. It replaces the former ``curl``/``wget`` subprocess
calls, so seqdd needs no external command-line tool and runs on Linux, macOS and Windows.

.. automodule:: seqdd.utils.net
   :members:
