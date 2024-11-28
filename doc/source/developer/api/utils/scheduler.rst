.. SeqDD documentation master file, created by
   sphinx-quickstart on Mon Oct 21 14:02:59 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.



scheduler API reference
=======================


The Abstract Class `Job`` is the base classes for all kind of Jobs.

.. inheritance-diagram::
      seqdd.utils.scheduler.Job
      seqdd.utils.scheduler.CmdLineJob
      seqdd.utils.scheduler.FunctionJob
   :parts: 1


The methods:

* :meth:`seqdd.utils.scheduler.Job.join`
* :meth:`seqdd.utils.scheduler.Job.start`
* :meth:`seqdd.utils.scheduler.Job.stop`

must be implemented in concrete classes

.. automodule:: seqdd.utils.scheduler
   :members:
   :private-members:
   :special-members:
