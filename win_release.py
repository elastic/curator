import os
import re
import sys
import shutil
import hashlib
import subprocess

ENVIRO = dict(os.environ)
PLATFORM = sys.platform
PYVER = str(sys.version_info[0]) + '.' + str(sys.version_info[1])
ARCHIVE_FMT = { 'win32': 'zip', 'linux': 'gztar', 'linux2': 'gztar' }
# This script simply takes the output of `python setup.py build_exe` and makes
# a compressed archive (zip for windows, tar.gz for Linux) for distribution.

def get_systype():
    return ENVIRO['_system_type'].lower() + '-' + ENVIRO['_system_arch'].lower()

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

def get_path(kind="sdist"):
    if kind == 'bdist_msi':
        build_path = os.path.join('.', 'dist', get_target(kind))
    else:
        if PLATFORM == 'win32':
            build_name = 'exe.win-' + ENVIRO['PROCESSOR_ARCHITECTURE'].lower() + '-' + PYVER
        else:
            build_name = 'exe.' + get_systype() + '-' + PYVER
        build_path = os.path.join('build', build_name)
    return build_path

def get_target(kind='sdist'):
    if PLATFORM == 'win32':
        target_name = 'curator-' + str(get_version()) + '-amd64'
    else:
        target_name = 'curator-' + str(get_version()) + '-' + get_systype()
    if kind == 'bdist_msi':
        target_name = 'elasticsearch-' + target_name + '.msi'
    return target_name

def check_target(kind="sdist"):
    target_path = os.path.join('.', get_target(kind))

    # Check to see if an older directory exists...
    if os.path.exists(target_path):
        print('An older build exists at {0}.  Please delete this before continuing.'.format(target_path))
        sys.exit(1)
    return target_path

def hash_package(fname):
    md5sum = hashlib.md5(open(fname, 'rb').read()).hexdigest()
    sha1sum = hashlib.sha1(open(fname, 'rb').read()).hexdigest()
    with open(fname + ".md5.txt", "w") as md5_file:
        md5_file.write("{0}".format(md5sum))
    with open(fname + ".sha1.txt", "w") as sha1_file:
        sha1_file.write("{0}".format(sha1sum))
    print('Archive: {0}'.format(fname))
    print('{0} = {1}'.format(fname + ".md5.txt", md5sum))
    print('{0} = {1}'.format(fname + ".sha1.txt", sha1sum))

def package_build(kind="sdist"):
    build_path = get_path(kind)
    #print("Looking for build_path: {0}".format(build_path))
    if os.path.exists(build_path):
        #print("I found the build_path: {0}".format(build_path))
    
        target_path = check_target(kind)
        #print("I found the target_path: {0}".format(target_path))
    
        # Ensure the rename went smoothly, then continue
        if kind == 'bdist_msi':
            shutil.move(build_path, target_path)
            fname = get_target(kind)
        else:
            shutil.copytree(build_path, target_path)
            if os.path.exists(target_path):
                #print("Build successfully renamed")
                shutil.make_archive('elasticsearch-' + get_target(), ARCHIVE_FMT[PLATFORM], '.', target_path)
                if PLATFORM == 'win32':
                    fname = 'elasticsearch-' + get_target() + '.zip'
                else:
                    fname = 'elasticsearch-' + get_target() + '.tar.gz'
                # Clean up directory if we made a viable archive.
                if os.path.exists(fname):
                    shutil.rmtree(target_path)
                else:
                    print('Something went wrong creating the archive {0}'.format(fname))
                    sys.exit(1)
        # Create hashes
        hash_package(fname)
    else:
        # We couldn't find a build_path
        print("Build not found.  Please run 'python setup.py build_exe' to create the build directory.")
        sys.exit(1)

def build_a_dist(kind="sdist"):
    args = ['python', 'setup.py', kind]
    os.system(' '.join(args))
################################################################################
# This pissses me off, because it worked in 3.7 and 3.8, but not 3.9. The args 
# are proper, but something about the subprocess doesn't work it ALWAYS
# complains that --upgrade-code is invalid, but if I run it manually on the
# command-line, it works just fine.
#
# It does work with os.system, so for now I am commenting this out.
#
#    process = subprocess.Popen(args)
#    process.wait(120)
#    if process.returncode != 0:
#        print('Build of {0} failed.'.format(kind))
#        sys.exit(1)
#    else:
#        package_build(kind)
    package_build(kind)

if PLATFORM == 'win32':
    build_a_dist('bdist_msi')
    build_a_dist('bdist')
elif PLATFORM == 'linux' or PLATFORM == 'linux2':
    build_a_dist('sdist')
else:
    # Unsupported platform?
    print('Your platform ({0}) is not yet supported for binary build/distribution.'.format(platform))
    sys.exit(1)

