#!/usr/bin/env python
# -*- coding: utf-8 -*-

import glob

def split_filenames():
    for filename in glob.glob('*.*'):
        if not filename.endswith('.py') and \
        not filename.endswith('.pyc') and \
        not filename.endswith('.result') and \
        not filename.startswith('__'):
            (stem, ext) = filename.split('.')
            yield (filename, stem, ext)


