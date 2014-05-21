==============
data-dispenser
==============

.. image:: https://badge.fury.io/py/data_dispenser.png
    :target: http://badge.fury.io/py/data_dispenser
    
.. image:: https://travis-ci.org/catherinedevlin/data_dispenser.png?branch=master
        :target: https://travis-ci.org/catherinedevlin/data_dispenser

.. image:: https://pypip.in/d/data_dispenser/badge.png
        :target: https://crate.io/packages/data_dispenser?version=latest


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
* actual Python data objects

Will work most reliably against filenames with extensions that indicate
the data format; otherwise data-dispenser may guess the input format wrong.

Data input formats supported
..........------............

* csv
* yaml (requires ``pyyaml``)
* json
* pickle
* ``eval``-able Python
* xml (experimental)

Multiple files
..............

Multiple file paths and/or file paths with wildcards will be
effectively concatenated into one large data source.

Load limits
...........

Large data sources could overwhelm your system's memory.  Passing a ``limit``
keyword to the ``Source`` constructor limits the rows returned from each
source.  For file paths with wildcards, the limit applies to each file
source, not to the number of file sources.


