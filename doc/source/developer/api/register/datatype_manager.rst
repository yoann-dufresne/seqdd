.. SeqDD - Sequence Data Downloader
    Authors: Yoann Dufresne
    Copyright © 2024  Institut Pasteur (Paris), and CNRS.
    See the COPYRIGHT file for details
    SeqDD is distributed under the terms of the GNU General Public License (GPLv3).
    See the COPYING file for details.


datatype_manager API reference
==============================

Dynamic discovery of the data types: it inspects the ``seqdd.register.data_type`` package, collects
every :class:`~seqdd.register.data_type.DataContainer` subclass, and instantiates each one with the
:class:`~seqdd.register.sources.DataSource` declared in its ``__init__`` annotation.

.. automodule:: seqdd.register.datatype_manager
   :members:
   :private-members:
   :special-members:
