
import os
import sys
import unittest
from argparse import ArgumentParser
from unittest import TestSuite


def discover(test_files: list[str] = None, test_root_path: str | None = None) -> TestSuite:
    if not test_root_path:
        test_root_path = os.path.dirname(__file__)

    if not test_files:
        suite = unittest.TestLoader().discover(test_root_path, pattern="test_*.py")

    else:
        test_files = [os.path.abspath(f) for f in test_files]
        test_files = [t for t in test_files if test_root_path in t]
        suite = unittest.TestSuite()
        for test_file in test_files:
            if os.path.exists(test_file):
                if os.path.isfile(test_file):
                    fpath, fname = os.path.split(test_file)
                    suite.addTests(unittest.TestLoader().discover(fpath, pattern=fname))
                elif os.path.isdir(test_file):
                    suite.addTests(unittest.TestLoader().discover(test_file))
            else:
                sys.stderr.write(f"{test_file} : no such file or directory\n")

    return suite


def run_tests(test_files: list[str], verbosity: int = 0) -> unittest.TestResult:
    """
    Execute Unit Tests

    :param test_files: the file names of tests to run.
                       if it is empty, discover recursively tests from 'tests' directory.
                       a test is python module with the test_*.py pattern
    :param verbosity: the verbosity of the output
    :return: True if the test passed successfully, False otherwise.
    """
    test_root_path = os.path.abspath(os.path.dirname(__file__))
    suite = discover(test_files, test_root_path)
    test_runner = unittest.TextTestRunner(verbosity=verbosity).run(suite)
    return test_runner


def main(args=None):
    args = args if not None else sys.argv[1:]

    parser = ArgumentParser()
    parser.add_argument("tests",
                        nargs='*',
                        default=False,
                        help="name of test to execute")

    parser.add_argument("-v", "--verbose",
                        dest="verbosity",
                        action="count",
                        help="set the verbosity level of output",
                        default=0
                        )

    args = parser.parse_args(args)

    test_runner = run_tests(args.tests, verbosity=args.verbosity)
    unit_results = test_runner.wasSuccessful()
    return unit_results


if __name__ == '__main__':

    unit_results = main(sys.argv[1:])
    if unit_results:
        sys.exit(0)
    else:
        sys.exit(1)
