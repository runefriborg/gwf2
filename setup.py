import os
from setuptools import setup

# Utility function to read the README file.  Used for the
# long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put
# a raw string in below ...

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "gwf",
    version = "0.4.0",

    author = "Thomas Mailund",
    author_email = "mailund@birc.au.dk",

    description = ("Grid WorkFlow - a make-like system for"
                   "submitting jobs through qsub."),
    long_description=read('README.md'),
    keywords = "qsub make",
    license = "GNU GPLv3",
    url = "https://github.com/mailund/gwf",

    packages=['gwf', 'tests'],
    scripts=['scripts/gwf',
             'scripts/gwf-status',
             'scripts/gwf-clean',
             'scripts/gwf-workflow-graph',
             'scripts/gwf-print-lists'],

    include_package_data=True,
    package_data={
        'gwf': ['templates/*.plot']
    },

    test_suite='tests',

    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Topic :: Utilities",
        "Topic :: System :: Distributed Computing",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License (GPLv3)",
        "Programming Language :: Python",
    ],
)
