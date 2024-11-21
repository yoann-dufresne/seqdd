.. SeqDD - Sequence Data Downloader
    Authors: Yoann Dufresne
    Copyright © 2024  Institut Pasteur (Paris), and CNRS.
    See the COPYRIGHT file for details
    SeqDD is distributed under the terms of the GNU General Public License (GPLv3).
    See the COPYING file for details.

.. _dev_installation:


============
Installation
============


dependencies
============

SeqDD use sphinx with some extensions to generate the documentation:
    - sphinx
    - sphinx_rtd_theme
    - sphinx-autodoc-typehints

SeqDD wil use coverage to compute the unit and functional test coverage

    - coverage

It's a good idea to use a python linter to check your code before to submit a pull/merge request

    - ruff

All these dependencies are managed by `pip`

Installation procedures
=======================


In a virtualenv
---------------

.. code-block:: bash

    # create a new virtaulenv
    python3 -m venv seqdd
    # activate it
    cd seqdd
    source bin/activate
    # clone/install the project in editable mode
    git clone git@github.com:yoann-dufresne/seqdd.git
    cd seqdd
    python3 -m pip install -e .[dev]

To exit the virtualenv just execute the `deactivate` command.

To use seqdd

.. code-block:: bash

    cd seqdd
    source seqdd/bin/activate

Then run seqdd.
