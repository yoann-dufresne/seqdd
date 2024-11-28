.. SeqDD documentation master file, created by
   sphinx-quickstart on Mon Oct 21 14:02:59 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.



src_manager API reference
=========================


Classes to handle data sources


There are 2 classes :class:`seqdd.register.src_manager.Downloader` and class:`seqdd.register.src_manager.SourceManager`.
The role of class:`seqdd.register.src_manager.Downloader` is to dynamically inspect the module `data_sources`
and register all DataSource concrete **classes**. it should be run once that why it implement a singleton pattern.

The role of `seqdd.register.src_manager.SourceManager` is to instantiate the DataSource classes and manage them.

.. automodule:: seqdd.register.src_manager
   :members:
   :private-members:
   :special-members:
