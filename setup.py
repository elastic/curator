import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def fread(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def get_version():
    VERSION = fread("VERSION").strip()
    build_number = os.environ.get('BUILD_NUMBER', None)
    if build_number:
        return VERSION + "b{}".format(build_number)
    return VERSION

setup(
    name = "elasticsearch-curator",
    version = get_version(),
    author = "Aaron Mildenstein",
    author_email = "aaron@mildensteins.com",
    description = "Tending your time-series indices in Elasticsearch",
    url = "http://github.com/elasticsearch/curator",
    license = "Apache License, Version 2.0",
    install_requires = [
        'elasticsearch >= 0.4.4'
    ],
    keywords = "elasticsearch time-series indexed index-expiry",
    packages = ["curator"],
    include_package_data=True,
    long_description=fread('README.md'),
    entry_points = {
        "console_scripts" : ["curator = curator.curator:main"]
    },
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrator",
        "License :: OSI Approved :: Apache Software License",
    ],
    test_suite = "curator.test.test_curator",
    tests_require = ["mock"]
)
