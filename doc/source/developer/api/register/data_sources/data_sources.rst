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

The Abstract Class Source is the base classes for all kind of data source

.. inheritance-diagram::
      seqdd.register.data_sources.DataSource
      seqdd.register.data_sources.ena.ENA
      seqdd.register.data_sources.ncbi.NCBI
      seqdd.register.data_sources.sra.SRA
      seqdd.register.data_sources.url.URL
      seqdd.register.data_sources.logan.Logan
   :parts: 1


Base class of all kind of Source

The methods:

* :meth:`seqdd.register.data_sources.DataSource.is_ready`
* :meth:`seqdd.register.data_sources.DataSource.src_delay_ready`
* :meth:`seqdd.register.data_sources.DataSource.jobs_from_accessions`

Must be implemented in concrete classes

.. autoclass:: seqdd.register.data_sources.DataSource
   :members:
   :private-members:
   :special-members:
