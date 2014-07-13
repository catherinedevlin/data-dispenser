"""
Creates a .result file for each file in this directory, for verifying results.
"""

from file_stems import split_filenames
from data_dispenser.sources import Source
import pickle

for (filename, stem, ext) in split_filenames():
    src = Source(filename)
    src._dump('%s.result' % stem)
    src = Source(filename)
    if ext != '.xls':
        with open('%s.pickle' % stem, 'wb') as outfile:
            pickle.dump(list(src), outfile)


