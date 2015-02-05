#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    name='data_dispenser',
    version='0.2.5.1',
    description='Loads data from various formats',
    long_description=readme + '\n\n' + history,
    author='Catherine Devlin',
    author_email='catherine.devlin@gmail.com',
    url='https://github.com/catherinedevlin/data_dispenser',
    packages=[
        'data_dispenser',
    ],
    package_dir={'data_dispenser': 'data_dispenser'},
    include_package_data=True,
    install_requires=[
    ],
    extras_require = {
        'Mongo': ['pymongo>=2.7', ],
        'yaml': ['pyyaml>=3.11', ],
        'web': ['requests>=2.3', ],
        'excel': ['xlrd>=0.9.3', ],
        },
    license="MIT",
    zip_safe=False,
    keywords='data_dispenser',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ],
    test_suite='tests',
)
