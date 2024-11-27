.. SeqDD - Sequence Data Downloader
    Authors: Yoann Dufresne
    Copyright © 2024  Institut Pasteur (Paris), and CNRS.
    See the COPYRIGHT file for details
    SeqDD is distributed under the terms of the GNU General Public License (GPLv3).
    See the COPYING file for details.

.. _dev_testing:

=======
Testing
=======

Tests
=====

The seqDD use `unittest` framework (included in the standard library) to test the code.

All tests stuff is in `tests` directory.

* The data directory contains data needed by the tests
* in the __init__.py file a SeqddTest class is defined and should be the base of all testcase use in the project
* each test_*.py represent a file containing unit or functional tests

To run all the tests (in the virtualenv)

.. code-block:: shell

    python -m unittest discover -s tests/

To increase verbosity of output

.. code-block:: shell

    python -m unittest discover -vv -s tests/

.. code-block:: text

    test_ena (test_ena.TestSource.test_ena) ... ok
    test_is_ready (test_ena.TestSource.test_is_ready) ... ok
    test_jobs_from_accessions (test_ena.TestSource.test_jobs_from_accessions) ... ok
    test_src_delay_ready (test_ena.TestSource.test_src_delay_ready) ... ok
    test_wait_may_turn (test_ena.TestSource.test_wait_may_turn) ... ok

    ----------------------------------------------------------------------
    Ran 5 tests in 0.001s

    OK

The tests must be in python file (`.py`) starting with with `test\_` \
It's possible to specify one or several test files, one module, or one class in a module or a method in a Test class.

Test the `test_ena` module

.. code-block:: shell

    python -m unittest -vv tests.test_ena

Test the test Class `TestEna` in the module `test_ena`

.. code-block:: shell

    python -m unittest -vv tests.test_ena.TestEna

.. code-block:: text

    test_ena (tests.test_ena.TestEna.test_ena) ... ok
    test_is_ready (tests.test_ena.TestEna.test_is_ready) ... ok
    test_jobs_from_accessions (tests.test_ena.TestEna.test_jobs_from_accessions) ... ok
    test_src_delay_ready (tests.test_ena.TestEna.test_src_delay_ready) ... ok
    test_wait_my_turn (tests.test_ena.TestEna.test_wait_my_turn) ... ok

    ----------------------------------------------------------------------
    Ran 5 tests in 0.001s

    OK

Test only the method `test_is_ready` from the test Class `TestEna` in module `test_ena`

.. code-block:: shell

    python -m unittest -vv tests.test_ena.TestEna.test_is_ready

.. code-block:: text

    test_is_ready (tests.test_ena.TestEna.test_is_ready) ... ok

    ----------------------------------------------------------------------
    Ran 1 test in 0.000s

    OK



Coverage
========

To compute the tests coverage, we use the `coverage <https://pypi.org/project/coverage/>`_ package.
The package is automatically installed if you have installed `seqdd` with the `dev` target see :ref:`installation <dev_installation>`
The coverage package is setup in the `pyproject.toml` configuration file

To compute the coverage

.. code-block:: shell

    coverage run

then display a report

.. code-block:: shell

    coverage report

.. code-block:: text

    Name                                 Stmts   Miss Branch BrPart  Cover
    ----------------------------------------------------------------------
    seqdd/__init__.py                        0      0      0      0   100%
    seqdd/errors.py                          4      0      0      0   100%
    seqdd/register/__init__.py               0      0      0      0   100%
    seqdd/register/sources/__init__.py      27      3      0      0    89%
    seqdd/register/sources/ena.py          162     91     60      5    40%
    seqdd/utils/__init__.py                  0      0      0      0   100%
    seqdd/utils/scheduler.py               202    131     66      0    26%
    ----------------------------------------------------------------------
    TOTAL                                  395    225    126      5    36%


If you want to force computation for python files in seqdd

.. code-block:: shell

    coverage run --source=seqdd

    ... inorout.py:461: CoverageWarning: --include is ignored because --source is set (include-ignored)
    self.warn("--include is ignored because --source is set", slug="include-ignored")
    test_ena (test_ena.TestEna.test_ena) ... ok
    test_is_ready (test_ena.TestEna.test_is_ready) ... ok
    test_jobs_from_accessions (test_ena.TestEna.test_jobs_from_accessions) ... ok
    test_move_and_clean_w_bad_md5 (test_ena.TestEna.test_move_and_clean_w_bad_md5) ... ok
    test_move_and_clean_w_good_md5 (test_ena.TestEna.test_move_and_clean_w_good_md5) ... ok
    test_move_and_clean_wo_md5 (test_ena.TestEna.test_move_and_clean_wo_md5) ... ok
    test_src_delay_ready (test_ena.TestEna.test_src_delay_ready) ... ok
    test_wait_my_turn (test_ena.TestEna.test_wait_my_turn) ... ok

    ----------------------------------------------------------------------
    Ran 8 tests in 0.010s

    OK

then

.. code-block:: shell

    coverage report

.. code-block:: text

    Name                                 Stmts   Miss  Cover
    --------------------------------------------------------
    seqdd/__init__.py                        0      0   100%
    seqdd/__main__.py                      137    137     0%
    seqdd/register/__init__.py               0      0   100%
    seqdd/register/reg_manager.py          112    112     0%
    seqdd/register/sources/__init__.py      27     15    44%
    seqdd/register/sources/ena.py          160    138    14%
    seqdd/register/sources/ncbi.py         142    142     0%
    seqdd/register/sources/sra.py          160    160     0%
    seqdd/register/sources/url.py           84     84     0%
    seqdd/register/src_manager.py           30     30     0%
    seqdd/utils/__init__.py                  0      0   100%
    seqdd/utils/download.py                 54     54     0%
    seqdd/utils/scheduler.py               202    152    25%
    --------------------------------------------------------
    TOTAL                                 1108   1024     8%

or generate a html report

.. code-block:: shell

    coverage html

The results are in the `htmlcov` directory. With you favourite web browser, open the `index.html` file.
