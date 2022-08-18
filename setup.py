import os
import re
import sys

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
    res = ['elasticsearch7==7.14.2' ]
    res.append('urllib3>=1.26.11,<2')
    res.append('requests>=2.28.1')
    res.append('boto3>=1.24.54')
    res.append('requests_aws4auth>=1.1.2')
    res.append('click==8.1.3')
    res.append('pyyaml==6.0.0')
    res.append('voluptuous>=0.13.1')
    res.append('certifi>=2022.6.15')
    res.append('six>=1.16.0')
    return res

try:
    ### cx_Freeze ###
    from cx_Freeze import setup, Executable

    try:
        import certifi
        cert_file = certifi.where()
    except ImportError:
        cert_file = ''
    # Dependencies are automatically detected, but it might need
    # fine tuning.


    base = 'Console'

    icon = None
    if os.path.exists('Elastic.ico'):
        icon = 'Elastic.ico'

    curator_exe = Executable(
        "run_curator.py",
        base=base,
        target_name = "curator",
    )
    curator_cli_exe = Executable(
        "run_singleton.py",
        base=base,
        target_name = "curator_cli",
    )
    repomgr_exe = Executable(
        "run_es_repo_mgr.py",
        base=base,
        target_name = "es_repo_mgr",
    )
    build_dict = { 
        "build_exe": dict(
            packages = [],
            excludes = [],
            include_files = [cert_file],
        )
    }
    setup(
        name = "elasticsearch-curator",
        version = get_version(),
        author = "Elastic",
        author_email = "info@elastic.co",
        description = "Tending your Elasticsearch indices",
        long_description=fread('README.rst'),
        url = "http://github.com/elastic/curator",
        download_url = "https://github.com/elastic/curator/tarball/v" + get_version(),
        license = "Apache License, Version 2.0",
        install_requires = get_install_requires(),
        setup_requires = get_install_requires(),
        keywords = "elasticsearch time-series indexed index-expiry",
        packages = ["curator"],
        include_package_data=True,
        entry_points = {
            "console_scripts" : [
                "curator = curator.cli:cli",
                "curator_cli = curator.curator_cli:main",
                "es_repo_mgr = curator.repomgrcli:repo_mgr_cli",
            ]
        },
        classifiers=[
            "Intended Audience :: Developers",
            "Intended Audience :: System Administrators",
            "License :: OSI Approved :: Apache Software License",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
        ],
        test_suite = "test.run_tests.run_all",
        tests_require = ["mock", "nose", "coverage", "nosexcover"],
        options = build_dict,
        executables = [curator_exe, curator_cli_exe, repomgr_exe]
    )

except ImportError:
    from setuptools import setup
    setup(
        name = "elasticsearch-curator",
        version = get_version(),
        author = "Elastic",
        author_email = "info@elastic.co",
        description = "Tending your Elasticsearch indices",
        long_description=fread('README.rst'),
        url = "http://github.com/elastic/curator",
        download_url = "https://github.com/elastic/curator/tarball/v" + get_version(),
        license = "Apache License, Version 2.0",
        install_requires = get_install_requires(),
        keywords = "elasticsearch time-series indexed index-expiry",
        packages = ["curator"],
        include_package_data=True,
        entry_points = {
            "console_scripts" : [
                "curator = curator.cli:cli",
                "curator_cli = curator.curator_cli:main",
                "es_repo_mgr = curator.repomgrcli:repo_mgr_cli",
            ]
        },
        classifiers=[
            "Intended Audience :: Developers",
            "Intended Audience :: System Administrators",
            "License :: OSI Approved :: Apache Software License",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
        ],
        test_suite = "test.run_tests.run_all",
        tests_require = ["mock", "nose", "coverage", "nosexcover"]
    )
