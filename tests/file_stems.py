#!/usr/bin/env python
# -*- coding: utf-8 -*-

import glob
import os.path

test_filenames = os.path.join(os.path.dirname(__file__), '*.*')
def split_filenames():
    for filename in glob.glob(test_filenames):
        if not filename.endswith('.py') and \
        not filename.endswith('.pyc') and \
        not filename.endswith('.result') and \
        not filename.endswith('.ipynb') and \
        not filename.endswith('.log') and \
        not filename.startswith('__'):
            (stem, ext) = filename.split('.', 1)
            yield (os.path.split(filename)[1], stem, ext)
            


