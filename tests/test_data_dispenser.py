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
import os.path
import time
import requests
import tempfile
import sqlite3

from data_dispenser import sources, sqlalchemy_table_sources
from tests.file_stems import split_filenames

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
        expectation_filename = here('%s.result' % stem)
        with open(expectation_filename) as infile:
            expected = eval(infile.read())
        yield (filename, stem, ext, expected)


def here(filename):
    return os.path.join(os.path.dirname(__file__), filename)


class TestURLreader(unittest.TestCase):

    def setUp(self):
        command = "python -m http.server"
        print(os.path.dirname(__file__))
        this_dir = os.path.dirname(os.path.realpath(__file__))
        self.webserver = subprocess.Popen(command.split(),
                                          cwd=this_dir)

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
            if '#' not in filename:
                url = "http://127.0.0.1:8000/%s" % filename
                src = self.keep_trying(url)
                result = list(src)
                self.assertEqual(result, expectation,
                                 msg="%s, from local webserver" % filename)


class Test_Sqlite(unittest.TestCase):

    def setUp(self):
        self.db = tempfile.NamedTemporaryFile()
        self.conn = sqlite3.connect(self.db.name)
        self.cursor = self.conn.cursor()
        sqls = [ """
        CREATE TABLE knights (
                name VARCHAR(10) NOT NULL,
                dob DATETIME,
                kg DECIMAL(6, 4),
                brave BOOLEAN NOT NULL,
                CHECK (brave IN (0, 1))
        )""",
        "INSERT INTO knights (name, dob, kg, brave) VALUES ('Lancelot', '0471-01-09 00:00:00', 82, 1)",
        "INSERT INTO knights (name, kg, brave) VALUES ('Gawain', 69.2, 1)",
        "INSERT INTO knights (name, dob, brave) VALUES ('Robin', '0471-01-09 00:00:00', 0)",
        "INSERT INTO knights (name, kg, brave) VALUES ('Reepacheep', 0.0691, 1)"
        ]
        for sql in sqls:
            self.cursor.execute(sql)
        self.conn.commit()

    def test_read_db(self):
        for tbl in sqlalchemy_table_sources('sqlite:///%s' % self.db.name):
            result = list(tbl)
            self.assertIn('Reepacheep', [r.name for r in result])


class Testdata_dispenser(unittest.TestCase):

    def setUp(self):
        pass

    def test_filenames(self):
        for (filename, stem, ext, expectation) in expectations():
            fieldnames = None
            if '#headers' in filename:
                fieldnames = stem.split('#')[2]
                try:
                    fieldnames = int(fieldnames)
                except ValueError:
                    fieldnames = fieldnames.split('-')
            src = sources.Source(here(filename), fieldnames=fieldnames)
            result = list(src)
            self.assertEqual(result, expectation,
                             msg="%s, by filename" % filename)

            # check that we can limit result size
            if expectation:
                src = sources.Source(here(filename), limit=1, fieldnames=fieldnames)
                self.assertEqual(list(src)[0], expectation[0],
                             msg='%s, limiting to 1')

            if ext == 'xls':
                continue

            # now test against an open file object
            with sources._open(here(filename)) as infile:
                src = sources.Source(infile, fieldnames=fieldnames)
                self.assertEqual(list(src), expectation,
                                 msg="%s, by file obj" % filename)

            # now test against the text contents
            if not filename.endswith('.pickle'):
                with open(here(filename)) as infile:
                    src = sources.Source(infile.read(), fieldnames=fieldnames)
                    self.assertEqual(list(src), expectation,
                                     msg="%s, by contents" % filename)

    def test_glob(self):
        with open(here('all_json.result')) as infile:
            expectation = eval(infile.read())
        src = sources.Source(here('*.json'))
        self.assertEqual(list(src), expectation,
                         'glob on *.json')
        with open(here('all_json_limit_1.result')) as infile:
            expectation = eval(infile.read())
        src = sources.Source(here('*.json'), limit=1)
        self.assertEqual(list(src), expectation,
                         'glob on *.json. limit=1')

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()

