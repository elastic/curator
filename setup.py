import os
import re
import sys
from setuptools import setup

# Utility function to read from file.
def fread(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def get_version():
    VERSIONFILE="curator/_version.py"
    verstrline = fread(VERSIONFILE).strip()
    vsre = r"^__version__ = ['\"]([^'\"]*)['\"]"
    mo = re.search(vsre, verstrline, re.M)
    if mo:
        VERSION = mo.group(1)
    else:
        raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))
    build_number = os.environ.get('CURATOR_BUILD_NUMBER', None)
    if build_number:
        return VERSION + "b{}".format(build_number)
    return VERSION

def get_install_requires():
    res = ['elasticsearch>=1.0.0,<2.0.0' ]
    res.append('click>=3.3')
    return res

setup(
    name = "elasticsearch-curator",
    version = get_version(),
    author = "Aaron Mildenstein",
    author_email = "aaron@mildensteins.com",
    description = "Tending your Elasticsearch indices",
    long_description=fread('README.md'),
    url = "http://github.com/elastic/curator",
    download_url = "https://github.com/elastic/curator/tarball/v" + get_version(),
    license = "Apache License, Version 2.0",
    install_requires = get_install_requires(),
    keywords = "elasticsearch time-series indexed index-expiry",
    packages = ["curator", "curator.api", "curator.cli"],
    include_package_data=True,
    entry_points = {
        "console_scripts" : ["curator = curator.curator:main",
                             "es_repo_mgr = curator.es_repo_mgr:main"]
    },
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
    ],
    test_suite = "test.run_tests.run_all",
    tests_require = ["mock", "nose", "coverage", "nosexcover"]
)
