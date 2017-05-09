import os
import re
import sys
import shutil
import hashlib

# This script simply takes the output of `python setup.py build_exe` and makes
# a compressed archive (zip for windows, tar.gz for Linux) for distribution.

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

archive_format = 'gztar'
enviro = dict(os.environ)
platform = sys.platform
pyver = str(sys.version_info[0]) + '.' + str(sys.version_info[1])
if platform == 'win32':
    # Win32 stuff
    archive_format = 'zip'
    build_name = 'exe.win-' + enviro['PROCESSOR_ARCHITECTURE'].lower() + '-' + pyver
    target_name = "curator-" + str(get_version()) + "-amd64"
elif platform == 'linux' or platform == 'linux2':
    sys_string = enviro['_system_type'].lower() + '-' + enviro['_system_arch'].lower()
    build_name = 'exe.' + sys_string + '-' + pyver
    target_name = "curator-" + str(get_version()) + "-" + sys_string
else:
    # Unsupported platform?
    print('Your platform ({0}) is not yet supported for binary build/distribution.'.format(platform))
    sys.exit(1)

#sys_string = sys_type + '-' + sys_arch
#build_name = 'exe.' + sys_string + '-' + pyver
#print('Expected build directory: {0}'.format(build_name))
build_path = os.path.join('build', build_name)

if os.path.exists(build_path):
    #print("I found the path: {0}".format(build_path))

    target_path = os.path.join('.', target_name)

    # Check to see if an older directory exists...
    if os.path.exists(target_path):
        print('An older build exists at {0}.  Please delete this before continuing.'.format(target_path))
        sys.exit(1)
    else:
        shutil.copytree(build_path, target_path)

    # Ensure the rename went smoothly, then continue
    if os.path.exists(target_path):
        #print("Build successfully renamed")
        if float(pyver) >= 2.7:
            shutil.make_archive('elasticsearch-' + target_name, archive_format, '.', target_path)
            if platform == 'win32':
                fname = 'elasticsearch-' + target_name + '.zip'
            else:
                fname = 'elasticsearch-' + target_name + '.tar.gz'
            # Clean up directory if we made a viable archive.
            if os.path.exists(fname):
                shutil.rmtree(target_path)
            else:
                print('Something went wrong creating the archive {0}'.format(fname))
                sys.exit(1)
            md5sum = hashlib.md5(open(fname, 'rb').read()).hexdigest()
            sha1sum = hashlib.sha1(open(fname, 'rb').read()).hexdigest()
            with open(fname + ".md5.txt", "w") as md5_file:
                md5_file.write("{0}".format(md5sum))
            with open(fname + ".sha1.txt", "w") as sha1_file:
                sha1_file.write("{0}".format(sha1sum))
            print('Archive: {0}'.format(fname))
            print('{0} = {1}'.format(fname + ".md5.txt", md5sum))
            print('{0} = {1}'.format(fname + ".sha1.txt", sha1sum))
        else:
            print('Your python version ({0}) is too old to use with shutil.make_archive.'.format(pyver))
            print('You can manually compress the {0} directory to achieve the same result.'.format(target_name))
else:
    # We couldn't find a build_path
    print("Build not found.  Please run 'python setup.py build_exe' to create the build directory.")
    sys.exit(1)
