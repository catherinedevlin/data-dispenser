#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Exposes ``Source``, a class which, when instantiated with
a source of row-like data, acts as a generator returning
OrderedDicts for each row.
"""
from collections import OrderedDict
from io import StringIO, BytesIO
import csv
import doctest
import glob
import itertools
import json
import logging
import os.path
import pickle
import pprint
import re
import sys
import urllib.parse
import xml.etree.ElementTree as et
try:
    import yaml
except ImportError:
    logging.info("Could not import ``pyyaml``; is it installed?``")
    yaml = None
try:
    from pymongo.collection import Collection as MongoCollection
except ImportError:
    logging.info("Could not import Collection from pymongo; is it installed?")
    MongoCollection = None.__class__
try:
    import requests
except ImportError:
    logging.info("Could not import ``requests``, will not load from URLs")
    requests = None
try:
    import xlrd
except ImportError:
    logging.info("Could not import ``xlrd``, will not load from .xls")
    xlrd = None
try:
    import bs4
except ImportError:
    logging.info("Could not import ``bs4 (beautifulsoup)``, will not load from HTML")
    bs4 = None

if yaml:
    def ordered_yaml_load(stream, Loader=yaml.Loader,
                          object_pairs_hook=OrderedDict, *args, **kwargs):
        """
        Preserves order with OrderedDict as yaml is loaded
        Thanks to coldfix
        http://stackoverflow.com/questions/5121931/in-python-how-can-you-load-yaml-mappings-as-ordereddicts
        """
        class OrderedLoader(Loader):
            pass
        OrderedLoader.add_constructor(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            lambda loader, node: object_pairs_hook(loader.construct_pairs(node)))
        result = yaml.load(stream, OrderedLoader)
        result = _ensure_rows(result)
        return iter(result)
else:
    def ordered_yaml_load(*args, **kwargs):
        raise ImportError('pyyaml not installed')

def _element_to_odict(element):
    """Given an ElementTree element, return a version of it
    expressed in OrderedDictionaries."""
    result = OrderedDict()
    #if hasattr(element, 'tag') and element.tag:
    result['tag'] = element.tag
    for (k, v) in element.items():
        result[k] = v
    for child in element:
        c = _element_to_odict(child)
        if child.tag in result:
            if isinstance(result[child.tag], list):
                result[child.tag].append(c)
            else:
                result[child.tag] = [result[child.tag], c]
        else:
            result[child.tag] = c
    if hasattr(element, 'text') and hasattr(element.text, 'strip'):
        text = element.text.strip()
        if text:
            if len(result) == 1: # just 'tag'
                return text
            result['text'] = text
    return result

def _first_list_in(element):
    """Often the meaningful part of an XML doc is nested inside
    one or more levels of elements that serve no purpose.
    This function strips a data tree down to the first list
    of actual data."""
    if isinstance(element, list):
        return element
    if not hasattr(element, 'items'):
        return None
    for (k, v) in element.items():
        if isinstance(v, list):
            return v
    for (k, v) in element.items():
        child_list_found = _first_list_in(v)
        if child_list_found:
            return child_list_found

def _ensure_rows(result):
    """data_dispenser is for rowlike sources.  If the data source
       evaluates to a single dict instead of a listlike object,
       transform a dict of dicts into a list of dicts, or 
       return a list (that contains the dict as its sole row).
       >>> pprint.pprint(_ensure_rows({"a": 1, "b": 2}))
       [{'a': 1, 'b': 2}]
       >>> pprint.pprint(_ensure_rows({"a": {"a1": 1, "a2": 2}, "b": {"b1": 1, "b2": 2}}))
       [{'a1': 1, 'a2': 2, 'name_': 'a'}, {'b1': 1, 'b2': 2, 'name_': 'b'}]
       
       otherwise just don't mess with it
       >>> pprint.pprint(_ensure_rows([{"a1": 1, "a2": 2}, {"b1": 1, "b2": 2}]))
       [{'a1': 1, 'a2': 2}, {'b1': 1, 'b2': 2}]
    """
    if isinstance(result, dict):
        if not result:
            result = []
        # if it's a dict of dicts, convert to a list of dicts
        if not [s for s in result.values() if not hasattr(s, 'keys')]:
            result = [dict(name_=k, **result[k]) for k in result]
        else:
            result = [result, ]
    return result

class ParseException(Exception):
    pass

# begin deserializers

def _eval_xml(target, *args, **kwargs):
    root = et.parse(target).getroot()
    data = _element_to_odict(root)
    data = _first_list_in(data)
    return iter(data)

def json_loader(target, *args, **kwargs):
    result = json.load(target, object_pairs_hook=OrderedDict)
    result = _ensure_rows(result)
    return iter(result)
json_loader.__name__ = 'json_loader'

def pickle_loader(target, *args, **kwargs):
    result = pickle.load(target)
    result = _ensure_rows(result)
    return iter(result)
pickle_loader.__name__ = 'pickle_loader'

def _eval_file_obj(target, *args, **kwargs):
    result = eval(target.read())
    if isinstance(result, list):
        for itm in result:
            yield itm
    else:
        yield result

def _interpret_fieldnames(target, fieldnames):
    try:
        fieldname_line_number = int(fieldnames)
    except (ValueError, TypeError):
        return fieldnames
    reader = csv.reader(target)
    if fieldnames == 0:
        num_columns = len(reader.__next__())
        fieldnames = ['Field%d' % (i+1) for i in range(num_columns)]
    else:
        for i in range(fieldname_line_number):
            fieldnames = reader.__next__()
    return fieldnames
 
def _eval_csv(target, fieldnames=None, *args, **kwargs):
    """
    Yields OrderedDicts from a CSV string
    """
    fieldnames = _interpret_fieldnames(target, fieldnames)
    reader = csv.DictReader(target, fieldnames=fieldnames)
    for row in reader:
        yield OrderedDict((k, row[k]) for k in reader.fieldnames)

def _table_score(tbl):
    n_rows = len((tbl.tbody or tbl).findAll('tr', recursive=False))
    n_headings = len((tbl.thead or tbl).tr.findAll('th', recursive=False))
    n_columns = len(tbl.tr.findAll('td', recursive=False))
    score = n_columns * 3 + n_headings * 10 + n_columns
    if tbl.thead:
        score += 3
    return score
    
def _html_to_odicts(html, *args, **kwargs):
    if not bs4:
        raise(ImportError, "BeautifulSoup4 not installed")
    soup = bs4.BeautifulSoup(html)
    tables = sorted(soup.find_all('table'), key=_table_score, reverse=True)
    if not tables:
        raise ParseException('No HTML tables found')
    tbl = tables[0]
    skips = 1
    if (tbl.thead or tbl).tr.th:
        headers = [th.text for th in (tbl.thead or tbl).tr.find_all('th', recursive=False)]
    else:
        headers = [td.text for td in (tbl.tbody or tbl).tr.find_all('td', recursive-False)]
    for (col_num, header) in enumerate(headers):
        header = header or "Field%d" % (col_num + 1)
    for tr in (tbl.tbody or tbl).find_all('tr', recursive=False):
        if skips > 0:
            skips -= 1
            continue
        row = [td.text for td in tr.find_all('td')]
        yield OrderedDict(zip(headers, row))
        
# end deserializers

def _open(filename):
    """Opens a file in binary mode if its name ends with 'pickle'"""
    if filename.lower().endswith('.pickle'):
        file_mode = 'rb'
    else:
        file_mode = 'rU'
    input_source = open(filename, file_mode)
    return input_source

def filename_from_url(url):
    return os.path.splitext(os.path.basename(urllib.parse.urlsplit(url).path))[0]

class NamedIter(object):
    "Hack to let us assign attributes to an iterator"

    def __init__(self, unnamed_iterator):
        self.__iter__ = unnamed_iterator.__iter__
        self.__next__ = unnamed_iterator.__next__


class Source(object):
    """
    A universal data generator that returns one "row" at
    a time, given a sources of row-like data.

    Usage::

        src = Source('mydata.csv')
        for row in src:
            print(row)

    By default, works with

    - csv
    - json
    - valid Python
    - Python pickle files
    - xml (experimental - returns first list of elements found)

    If ``pyyaml``, ``pymongo`` are installed, recognizes
    those formats as well.

    Can impose a limit on number of rows returned, which may
    save on memory.

        src = Source('mydata.csv', limit=10)

    For XML, we assume that the data of interest is a list
    and drill through any outer layers that may enclose a
    list, discarding them.  (XML support experimental)
    """

    eval_funcs_by_ext = {'.py': [_eval_file_obj, ],
                         '.json': [json_loader, ],
                         '.yaml': [ordered_yaml_load, ],
                         '.yml': [ordered_yaml_load, ],
                         '.csv': [_eval_csv, ],
                         '.xml': [_eval_xml, ],
                         '.html': [_html_to_odicts, ],
                         '.htm': [_html_to_odicts, ],
                         '.pickle': [pickle_loader, ],
                         }
    eval_funcs_by_ext['*'] = eval_funcs_by_ext['.pickle'] + \
                             [_eval_file_obj, ] + \
                             eval_funcs_by_ext['.html'] + \
                             eval_funcs_by_ext['.xml'] + \
                             eval_funcs_by_ext['.json'] + \
                             eval_funcs_by_ext['.yaml'] + \
                             eval_funcs_by_ext['.csv']
    table_count = 0

    def _source_is_generator(self, src):
        if hasattr(src, 'name'):
            self.table_name = src.name
        self.generator = src
        return

    def _source_is_mongo(self, src):
        self.table_name = src.name
        self.generator = src.find()
        return

    def _deserialize(self, open_file):
        self.file = open_file
        for deserializer in self.deserializers:
            self.file.seek(0)
            try:
                self.generator = deserializer(open_file, fieldnames=self.fieldnames)
                row_1 = self.generator.__next__()
                self.file.seek(0)
                if row_1:
                    if (deserializer == ordered_yaml_load and isinstance(row_1, str)
                        and len(row_1) == 1):
                        logging.info('false hit: reading `yaml` as a single string')
                        continue
                    self.file.seek(0)
                    self.generator = deserializer(open_file, fieldnames=self.fieldnames)
                    self.deserializer = deserializer
                    return
                else:
                    logging.info('%s found no items in first row of %s'
                                 % (deserializer, open_file))
            except StopIteration:
                self.file.seek(0)
                self.deserializer = deserializer
                return
            except Exception as e:
                logging.info('%s failed to deserialize %s' % (deserializer, open_file))
                logging.info(str(e))
        raise SyntaxError("%s: Could not deserialize %s (tried %s)" % (
            self.table_name, open_file, ", ".join(self.deserializers)))

    def _source_is_path(self, src):
        (file_path, file_extension) = os.path.splitext(src)
        self.table_name = os.path.split(file_path)[1]
        logging.info('Reading data from %s' % src)
        file_extension = file_extension.lower()
        self.deserializers = self.eval_funcs_by_ext.get(
            file_extension,
            self.eval_funcs_by_ext['*'])
        self.deserializer = None
        input_source = _open(src)
        self._deserialize(input_source)

    def _multiple_sources(self, sources):
        subsources = [Source(s, limit=self.limit) for s in sources]
        self.limit = None  # impose limit only on the subsources
        self.generator = itertools.chain.from_iterable(subsources)

    _actual_ext_finder = re.compile(r"^(\.[A-Za-z]*)")
    def _source_is_url(self, src):
        self.table_name = filename_from_url(src)
        if not requests:
            raise ImportError('must ``pip install requests to read from web``')
        (core_url, ext) = os.path.splitext(src)
        ext = self._actual_ext_finder.search(ext).group(1)
        ext = ext.lower()
        response = requests.get(src)
        if ext.endswith('.xls'):
            return self._source_is_excel(response.content)
        self.deserializers = self.eval_funcs_by_ext.get(ext or '*')
        if ext == '.pickle':
            self._deserialize(BytesIO(response.content))
        else:
            content = response.content.decode(response.encoding or response.apparent_encoding)
            self._deserialize(StringIO(content))

    def _source_is_open_file(self, src):
        if hasattr(src, 'name'):
            self.table_name = src.name
        self.deserializers = self.eval_funcs_by_ext['*']
        self._deserialize(src)

    def _source_is_excel(self, spreadsheet):
        if not xlrd:
            raise ImportError('must ``pip install xlrd``')
        if len(spreadsheet) < 84 and spreadsheet.endswith('xls'):
            workbook = xlrd.open_workbook(spreadsheet)
            name = spreadsheet
        else:
            workbook = xlrd.open_workbook(file_contents=spreadsheet)
            name = "excel"
        generators = []
        for sheet in workbook.sheets():
            headings = ["Col%d" % c for c in range(1, sheet.ncols + 1)]
            data = []
            for row_n in range(sheet.nrows):
                row_has_data = max(bool(v) for v in sheet.row_values(row_n))
                if row_has_data:
                    headings = [heading if heading else default_heading
                                for (heading, default_heading) 
                                in itertools.zip_longest(sheet.row_values(row_n), headings)]
                    row_n += 1
                    break
            data = [OrderedDict(zip(headings, sheet.row_values(r)))
                                for r in range(row_n,sheet.nrows)]
            generator = NamedIter(iter(data))
            generator.name = "%s-%s" % (name, sheet.name)
            generators.append(generator)
        self._multiple_sources(generators)

    def __init__(self, src, limit=None, fieldnames=None):
        """
        For ``.csv`` and ``.xls``, field names will be taken from 
        the first line of data found - unless ``fieldnames`` is given,
        in which case, it will override.  For ``.xls``, ``fieldnames``
        may be an integer, in which case it will be the (1-based) row number
        field names will be taken from (rows before that will be discarded).
        """
        self.counter = 0
        self.limit = limit
        self.deserializers = []
        self.table_name = 'Table%d' % (Source.table_count)
        self.fieldnames = fieldnames
        Source.table_count += 1
        if isinstance(src, MongoCollection):
            self._source_is_mongo(src)
            return
        if hasattr(src, 'startswith') and (
                src.startswith("http://") or src.startswith("https://")):
            self._source_is_url(src)
            return
        if hasattr(src, 'read'):    # open file
            self._source_is_open_file(src)
            return
        if hasattr(src, '__next__'):
            self._source_is_generator(src)
            return
        try:
            if os.path.isfile(src):
                if src.endswith('.xls'):
                    self._source_is_excel(src)
                else:
                    self._source_is_path(src)
                return
        except TypeError:
            pass
        if hasattr(src, 'startswith') and src.startswith('http'):
            self._source_is_url(src)
        try:
            data = eval(src)
            if not hasattr(data, '__next__'):
                data = iter(data)
            self._source_is_generator(data)
            return
        except:
            pass
        try:
            sources = sorted(glob.glob(src))
            if sources:
                self._multiple_sources(sources)
                return
        except:
            pass
        try:
            string_file = StringIO(src.strip())
            self._source_is_open_file(string_file)
            return
        except:
            pass
        raise NotImplementedError('Could not read data source %s of type %s' %
                                  (str(src), str(type(src))))

    def __iter__(self):
        return self

    def __next__(self):
        self.counter += 1
        if self.limit and (self.counter > self.limit):
            raise StopIteration
        return self.generator.__next__()

    def _dump(self, filename):
        all_data = list(self)
        with open(filename, 'w') as outfile:
            outfile.write(pprint.pformat(all_data))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        for target in sys.argv[1:]:
            pprint.pprint(list(Source(target)))
    else:
        doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
