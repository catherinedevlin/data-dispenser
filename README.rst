==============
data-dispenser
==============

.. image: https://travis-ci.org/catherinedevlin/data-dispenser.svg?branch=master
   :alt: Travis build status

Given a source of rowlike data, acts as a generator of OrderedDicts.

    Usage::

        src = Source('mydata.csv')
        for row in src:
            print(row)

data-dispenser thus serves as a single API for a variety of data sources.

* Free software: MIT license

Data source types supported
...........................

* file names / paths
* open file objects
* pymongo Collection objects
* strings interpretable as data 
* URLs beginning with http:// or https://

Will work most reliably against filenames with extensions that indicate
the data format; otherwise data-dispenser may guess the input format wrong.

Data input formats supported
............................

* csv
* yaml (requires ``pyyaml``)
* json
* pickle
* ``eval``-able Python
* xls
* xml (experimental)

Multiple files
..............

File paths with wildcards will be
effectively concatenated into one large data source.

Load limits
...........

Large data sources could overwhelm your system's memory.  Passing a ``limit``
keyword to the ``Source`` constructor limits the rows returned from each
source.  For file paths with wildcards, the limit applies to each file
source, not to the number of file sources.

Code
----

https://pypi.python.org/pypi/data_dispenser

Source and bug tracker
----------------------

https://github.com/catherinedevlin/data-dispenser

