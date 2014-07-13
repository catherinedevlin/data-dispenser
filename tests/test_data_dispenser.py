#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_universal_data_loader
----------------------------------

Tests for `universal_data_loader` module.
"""

import unittest
import subprocess
from collections import OrderedDict
import pymongo
import datetime
import time
import requests

from data_dispenser import sources
from file_stems import split_filenames

class TestReadMongo(unittest.TestCase):

    def setUp(self):
        database = pymongo.Connection().test_db
        tbl_name = 'tbl%f' % datetime.datetime.now().timestamp()
        self.tbl = database[tbl_name]
        self.tbl.insert({'field1': 'val1', 'field2': 2})
        self.tbl.insert({'field1': 'val1-1', })

    def test_read(self):
        src = sources.Source(self.tbl)
        data = list(src)
        self.assertEqual(data[0]['field1'], 'val1')
        self.assertEqual(data[0]['field2'], 2)
        self.assertEqual(data[1]['field1'], 'val1-1')

    def tearDown(self):
        self.tbl.drop()


def expectations():
    for (filename, stem, ext) in split_filenames():
        print("\n\n\nTesting %s\n***********\n\n\n" % filename)
        expectation_filename = '%s.result' % stem
        with open(expectation_filename) as infile:
            expected = eval(infile.read())
        yield (filename, stem, ext, expected)


class TestURLreader(unittest.TestCase):

    def setUp(self):
        self.webserver = subprocess.Popen("python -m http.server".split())

    def tearDown(self):
        self.webserver.terminate()

    def keep_trying(self, url):
        # local test webserver doesn't like being overtasked.
        for tries in range(5):
            try:
                src = sources.Source(url)
                return src
            except requests.exceptions.ConnectionError as e:
                if "Max retries exceeded" not in str(e):
                    raise(e)
                time.sleep(2**tries)
                remembered_err = e
        raise(remembered_err)

    def test_filenames(self):
        for (filename, stem, ext, expectation) in expectations():
            url = "http://127.0.0.1:8000/%s" % filename
            src = self.keep_trying(url)
            self.assertEqual(list(src), expectation,
                             msg="%s, from local webserver" % filename)
        

class Testdata_dispenser(unittest.TestCase):

    def setUp(self):
        pass

    def test_filenames(self):
        for (filename, stem, ext, expectation) in expectations():
            src = sources.Source(filename)
            self.assertEqual(list(src), expectation,
                             msg="%s, by filename" % filename)

            # check that we can limit result size
            src = sources.Source(filename, limit=1)
            self.assertEqual(list(src), expectation[:1],
                             msg='%s, limiting to 1')

            if ext == 'xls':
                continue

            # now test against an open file object
            with sources._open(filename) as infile:
                src = sources.Source(infile)
                self.assertEqual(list(src), expectation,
                                 msg="%s, by file obj" % filename)

            # now test against the text contents
            if not filename.endswith('.pickle'):
                with open(filename) as infile:
                    src = sources.Source(infile.read())
                    self.assertEqual(list(src), expectation,
                                     msg="%s, by contents" % filename)

    def test_glob(self):
        with open('all_json.result') as infile:
            expectation = eval(infile.read())
        src = sources.Source('*.json')
        self.assertEqual(list(src), expectation,
                         'glob on *.json')
        with open('all_json_limit_1.result') as infile:
            expectation = eval(infile.read())
        src = sources.Source('*.json', limit=1)
        self.assertEqual(list(src), expectation,
                         'glob on *.json. limit=1')

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

