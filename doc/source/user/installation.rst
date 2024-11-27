.. SeqDD - Sequence Data Downloader
    Authors: Yoann Dufresne
    Copyright © 2024  Institut Pasteur (Paris), and CNRS.
    See the COPYRIGHT file for details
    SeqDD is distributed under the terms of the GNU General Public License (GPLv3).
    See the COPYING file for details.

.. _usr_installation:


============
Installation
============


From the sources
================

Get the sources

.. code-block:: shell

    git clone https://github.com/yoann-dufresne/seqdd.git seqdd


The installation itself

.. code-block:: shell

    cd seqdd
    python3 -m pip install .

If you do not have the privileges to perform a system-wide installation,
you can either install it in your home directory or
use a `virtual environment <https://virtualenv.pypa.io/en/stable/>`_.

installation in your home directory
"""""""""""""""""""""""""""""""""""

.. code-block:: shell

    python3 -m pip install --user .


installation in a virtualenv
""""""""""""""""""""""""""""

Creates a `virtualenv <https://virtualenv.pypa.io/en/stable/>`_

.. code-block:: shell

    python3 -m venv SeqDD_env
    cd SeqDD_env
    source bin/activate

get the sources

.. code-block:: shell

    mkdir src
    cd src
    git clone  https://github.com/yoann-dufresne/seqdd.git

install it
""""""""""

.. code-block:: text

    python3 -m pip install macsyfinder

Then you can use seqdd

To exit the virtualenv just execute the `deactivate` command.
To run `SeqDD`, you need to activate the virtualenv:

.. code-block:: bash

    source SeqDD_env/bin/activate


Then run `seqdd`.


.. note::
  Super-user privileges (*i.e.*, `sudo`) are necessary if you want to install the program in the general file architecture.


.. note::
  If you do not have the privileges, or if you do not want to install SeqDD in the Python libraries of your system,
  you can install SeqDD in a virtual environment (http://www.virtualenv.org/).
