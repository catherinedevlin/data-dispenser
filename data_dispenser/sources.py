#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Exposes ``Source``, a class which, when instantiated with
a source of row-like data, acts as a generator returning
OrderedDicts for each row.
"""
from collections import OrderedDict
from io import StringIO
import csv
import doctest
import glob
import itertools
import json
import logging
import os.path
import pickle
import pprint
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

if yaml:
    def ordered_yaml_load(stream, Loader=yaml.Loader,
                          object_pairs_hook=OrderedDict):
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
    def ordered_yaml_load(*arg, **kwarg):
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
       return a list (that contains the dict as its sole row). """
    if isinstance(result, dict):
        result = [result, ]
    return result


def _eval_xml(target):
    root = et.parse(target).getroot()
    data = _element_to_odict(root)
    data = _first_list_in(data)
    return iter(data)

def json_loader(target):
    result = json.load(target, object_pairs_hook=OrderedDict)
    result = _ensure_rows(result)
    return iter(result)
json_loader.__name__ = 'json_loader'

def pickle_loader(target):
    result = pickle.load(target)
    result = _ensure_rows(result)
    return iter(result)
pickle_loader.__name__ = 'pickle_loader'

def _eval_file_obj(target):
    result = eval(target.read())
    if isinstance(result, list):
        for itm in result:
            yield itm
    else:
        yield result

def _eval_csv(target):
    """
    Yields OrderedDicts from a CSV string
    """
    reader = csv.DictReader(target)
    for row in reader:
        yield OrderedDict((k, row[k]) for k in reader.fieldnames)

def _open(filename):
    """Opens a file in binary mode if its name ends with 'pickle'"""
    if filename.lower().endswith('.pickle'):
        file_mode = 'rb'
    else:
        file_mode = 'rU'
    input_source = open(filename, file_mode)
    return input_source

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
                         '.csv': [_eval_csv, ],
                         '.xml': [_eval_xml, ],
                         '.pickle': [pickle_loader, ],
                         }
    eval_funcs_by_ext['*'] = eval_funcs_by_ext['.pickle'] + \
                             [_eval_file_obj, ] + \
                             eval_funcs_by_ext['.xml'] + \
                             eval_funcs_by_ext['.json'] + \
                             eval_funcs_by_ext['.yaml'] + \
                             eval_funcs_by_ext['.csv']
    table_count = 0

    def _source_is_generator(self, src):
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
                self.generator = deserializer(open_file)
                row_1 = self.generator.__next__()
                self.file.seek(0)
                if row_1:
                    if (deserializer == ordered_yaml_load and isinstance(row_1, str)
                        and len(row_1) == 1):
                        logging.info('false hit: reading `yaml` as a single string')
                        continue
                    self.file.seek(0)
                    self.generator = deserializer(open_file)
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

    def _source_is_glob(self, sources):
        subsources = [Source(s, limit=self.limit) for s in sources]
        self.limit = None  # impose limit only on the subsources
        self.generator = itertools.chain.from_iterable(subsources)

    def _source_is_open_file(self, src):
        if hasattr(src, 'name'):
            self.table_name = src.name
        self.deserializers = self.eval_funcs_by_ext['*']
        self._deserialize(src)

    def __init__(self, src, limit=None):
        self.counter = 0
        self.limit = limit
        self.deserializers = []
        self.table_name = 'Table%d' % (Source.table_count)
        Source.table_count += 1
        if isinstance(src, MongoCollection):
            self._source_is_mongo(src)
            return
        if hasattr(src, 'read'):    # open file
            self._source_is_open_file(src)
            return
        if hasattr(src, '__next__'):
            self._source_is_generator(src)
            return
        try:
            if os.path.isfile(src):
                self._source_is_path(src)
                return
        except TypeError:
            pass
        try:
            data = eval(src)
            self._source_is_generator(data)
            return
        except:
            pass
        try:
            sources = glob.glob(src)
            if sources:
                self._source_is_glob(sources)
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
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
