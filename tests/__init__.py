import os
import sys
import unittest
import hashlib
from functools import partial
import json
import re
import logging
from io import StringIO
from contextlib import contextmanager


class SeqddTest(unittest.TestCase):

    _tests_dir = os.path.normpath(os.path.dirname(__file__))
    _data_dir = os.path.join(_tests_dir, "data")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    @classmethod
    def find_data(cls, *args):
        data_path = os.path.join(cls._data_dir, *args)
        if os.path.exists(data_path):
            return data_path
        else:
            raise IOError("data '{}' does not exists".format(data_path))


    @contextmanager
    def catch_io(self, out=False, err=False):
        """
        Catch stderr and stdout of the code running within this block.
        """
        old_out = sys.stdout
        new_out = old_out
        old_err = sys.stderr
        new_err = old_err
        if out:
            new_out = StringIO()
        if err:
            new_err = StringIO()
        try:
            sys.stdout, sys.stderr = new_out, new_err
            yield sys.stdout, sys.stderr
        finally:
            sys.stdout, sys.stderr = old_out, old_err

    @staticmethod
    def fake_exit(*args, **kwargs):
        returncode = args[0]
        raise TypeError(returncode)

    @staticmethod
    def mute_call(call_ori):
        """
        hmmsearch or prodigal write lot of things on stderr or stdout
        which noise the unit test output
        So I replace the `call` function in module integron_finder
        by a wrapper which call the original function but add redirect stderr and stdout
        in dev_null
        :return: wrapper around call function
        :rtype: function
        """
        def wrapper(*args, **kwargs):
            with open(os.devnull, 'w') as f:
                kwargs['stderr'] = f
                kwargs['stdout'] = f
                res = call_ori(*args, **kwargs)
            return res
        return wrapper

    @staticmethod
    def remove_red_ansi_color(colored_msg):
        red_pattern = r"^\x1b\[0?1;31m(.*)\x1b\[0m$"
        msg = re.match(red_pattern, colored_msg).groups()[0]
        return msg


    def assertFileEqual(self, f1, f2, comment=None, skip_line=None, msg=None):
        self.maxDiff = None
        # the StringIO does not support context in python2.7
        # so we can use the following statement only in python3
        from itertools import zip_longest
        with open(f1) if isinstance(f1, str) else f1 as fh1, open(f2) if isinstance(f2, str) else f2 as fh2:
            for l1, l2 in zip_longest(fh1, fh2):
                if l1 and l2:
                    if comment and l1.startswith(comment) and l2.startswith(comment):
                        continue
                    elif skip_line:
                        if re.search(skip_line, l1) and re.search(skip_line, l2):
                            continue
                        try:
                            self.assertEqual(l1, l2, msg)
                        except AssertionError as err:
                            raise AssertionError(f"{fh1.name} and {fh2.name} differ:\n {err}")
                    try:
                        self.assertEqual(l1, l2, msg)
                    except AssertionError as err:
                        raise AssertionError(f"{fh1.name} and {fh2.name} differ:\n {err}")
                elif l1:  # and not l2
                    raise self.failureException(f"{fh1.name} is longer than {fh2.name}")
                elif l2:  # and not l1
                    raise self.failureException(f"{fh2.name} is longer than {fh1.name}")


    def assertJsonEqual(self, json_file_1, json_file_2, max_diff=640):
        with open(json_file_1) as f1:
            j1 = json.load(f1)
        with open(json_file_2) as f2:
            j2 = json.load(f2)

        self.maxDiff = max_diff
        self.assertListEqual(j1, j2)



    @staticmethod
    def md5sum(file_=None, str_=None):
        """Compute md5 checksum.

        :param file_: the name of the file to compute the checksum for
        :type file_: str
        :param str_: the string to compute the checksum for
        :type str_: str
        """
        assert not (file_ and str_)

        d = hashlib.md5()

        if file_:
            with open(file_, mode='rb') as f:
                for buf in iter(partial(f.read, 128), b''):
                    d.update(buf)
        elif str_:
            assert isinstance(str_, str)
            d.update(str_)
        else:
            assert False
        return d.hexdigest()


    @contextmanager
    def catch_log(self, log_name='seqdd'):
        logger = logging.getLogger(log_name)
        handlers_ori = logger.handlers
        fake_handler = logging.StreamHandler(StringIO())
        try:
            logger.handlers = [fake_handler]
            yield LoggerWrapper(logger)
        finally:
            logger.handlers = handlers_ori


class LoggerWrapper(object):

    def __init__(self, logger):
        self.logger = logger

    def __getattr__(self, item):
        return getattr(self.logger, item)

    def get_value(self):
        return self.logger.handlers[0].stream.getvalue()
