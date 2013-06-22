import os
from setuptools import setup

# Utility function to read the README file.  Used for the
# long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to
# put a raw string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "gwf",
    version = "0.0.1",
    author = "Thomas Mailund",
    author_email = "mailund@birc.au.dk",
    description = ("Grid WorkFlow - a make-like system for"
                   "submitting jobs through qsub."),
    license = "Free for all purposes",
    keywords = "qsub make",
    url = "https://github.com/mailund/gwf",
    packages=['gwf', 'tests'],
    long_description=read('README'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Topic :: Utilities",
    ],
)