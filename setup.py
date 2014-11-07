import os
import sys
from setuptools import setup

# Utility function to read from file.
def fread(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def get_version():
    VERSION = fread("VERSION").strip()
    build_number = os.environ.get('CURATOR_BUILD_NUMBER', None)
    if build_number:
        return VERSION + "b{}".format(build_number)
    return VERSION

def get_install_requires():
    res = ['elasticsearch>=1.0.0,<2.0.0' ]
    if sys.version_info < (2, 7):
        res.append('argparse>=1.1.0')
    if (3, 0) <= sys.version_info < (3, 2):
        res.append('argparse>=1.1.0')
    return res

setup(
    name = "elasticsearch-curator",
    version = get_version(),
    author = "Aaron Mildenstein",
    author_email = "aaron@mildensteins.com",
    description = "Tending your time-series indices in Elasticsearch",
    long_description=fread('README.md'),
    url = "http://github.com/elasticsearch/curator",
    download_url = "https://github.com/elasticsearch/curator/tarball/v" + get_version(),
    license = "Apache License, Version 2.0",
    install_requires = get_install_requires(),
    keywords = "elasticsearch time-series indexed index-expiry",
    packages = ["curator"],
    include_package_data=True,
    entry_points = {
        "console_scripts" : ["curator = curator.curator_script:main",
                             "es_repo_mgr = curator.es_repo_mgr:main"]
    },
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
    ],
    test_suite = "test_curator.run_tests.run_all",
    tests_require = ["mock", "nose", "coverage", "nosexcover"]
)
