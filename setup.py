from setuptools import setup, find_packages
from codecs import open
from os import path

"""
police-eis: police early intervention system
Copyright (C) 2016: Center for Data Science
and Public Policy <dsapp.org>
"""

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='police-eis',

    # Versions should comply with PEP440
    version='0.0.1',
    description='Police Early Intervention System',
    long_description=long_description,
    url='https://github.com/dssg/police-eis',
    author='Jennifer Helsby',
    author_email='jen@redshiftzero.com',
    license='GPL v3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Other Audience',
        'Topic :: Scientific/Engineering :: Information Analysis', 
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='police analytics machine learning misconduct prediction',

    packages=find_packages(exclude=['images', 'docs', 'tests*']),

    install_requires=['numpy', 'scipy', 'pandas', 'pyyaml',
                      'sqlalchemy', 'datetime', 'scikit-learn',
                      'psycopg2'],

    # Dev package requirements
    # $ pip install -e .[dev,test]
    extras_require={
        'dev': ['nose'],
        'test': ['nose'],
    },

    package_data={
        'sample': ['example_default_profile.yaml'],
        'sample': ['example_police_dept.yaml'],
        'sample': ['default.yaml']
    },

    test_suite='nose.collector',
    tests_require=['nose']

)
