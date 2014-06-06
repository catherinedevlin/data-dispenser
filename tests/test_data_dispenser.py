#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_universal_data_loader
----------------------------------

Tests for `universal_data_loader` module.
"""

import unittest
from collections import OrderedDict
import pymongo
import datetime

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


class Testdata_dispenser(unittest.TestCase):

    def setUp(self):
        pass

    def test_filenames(self):
        for (filename, stem, ext) in split_filenames():
            print(filename)
            expectation_filename = '%s.result' % stem
            with open(expectation_filename) as infile:
                expectation = eval(infile.read())
            src = sources.Source(filename)
            self.assertEqual(list(src), expectation,
                             msg="%s, by filename" % filename)

            # check that we can limit result size
            src = sources.Source(filename, limit=1)
            self.assertEqual(list(src), expectation[:1],
                             msg='%s, limiting to 1')

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

class TestReadFromWeb(unittest.TestCase):

    def test_read(self):
        url = 'http://www.whitehouse.gov/sites/default/files/omb/budget/fy2015/assets/hist01z1.xls'
        src = sources.Source(url)
        with open('budgetsummary.xls') as infile:
            expectation = eval(infile.read())
        self.assertEqual(list(src), expectation, 'Reading %s' % url)

if __name__ == '__main__':
    unittest.main()

