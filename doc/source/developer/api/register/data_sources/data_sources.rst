.. SeqDD - Sequence Data Downloader
    Authors: Yoann Dufresne
    Copyright © 2024  Institut Pasteur (Paris), and CNRS.
    See the COPYRIGHT file for details
    SeqDD is distributed under the terms of the GNU General Public License (GPLv3).
    See the COPYING file for details.


.. _sources_api:

==========
Source API
==========

A *source* is a server from which data can be downloaded (ENA, RefSeq, an arbitrary URL server…).
The abstract class :class:`seqdd.register.sources.DataSource` is the base class for all sources;
it also handles the per-source rate limiting.

Concrete subclasses must implement :meth:`seqdd.register.sources.DataSource.jobs_from_accessions`.

.. autoclass:: seqdd.register.sources.DataSource
   :members:
   :private-members:
   :special-members:
