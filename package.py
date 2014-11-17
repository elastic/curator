#!/usr/bin/python

import os, shutil, sys, traceback
from subprocess import call

# Utility function to read from file.
def fread(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def get_version():
    VERSION = fread("VERSION").strip()
    build_number = os.environ.get('CURATOR_BUILD_NUMBER', None)
    if build_number:
        return VERSION + "b{}".format(build_number)
    return VERSION
    
def create_temp_directory():
	shutil.rmtree("./tmp", ignore_errors=True)
	os.makedirs("./tmp")
    
def download_dependencies():
	call(["pip", "install", "--no-use-wheel", "--download=./tmp", "elasticsearch"])
	call(["pip", "install", "--no-use-wheel", "--download=./tmp", "argparse"])
    
def bundle_install():
	call(["bundle", "install"])
    
def execute_fpm():
	call(["fpm",
		"-s","dir",
		"-t",sys.argv[1],
		"--name","elasticsearch-curator",
		"--description","Curator: Tending your time-series indices in Elasticsearch",
		"--vendor","Elasticsearch",
		"--maintainer","Elasticsearch",
		"--version",get_version(),
		"--url","elasticsearch.org",
		"--prefix","usr/share/elasticsearch/curator",
		"--inputs","fpm/package-inputs",
		"--force",
		"--after-install","fpm/install-local.sh"
	])
	
def main():
	try:
		if len(sys.argv) < 2:
			raise Exception()
		else:
			create_temp_directory()
			bundle_install()
			download_dependencies()
			execute_fpm()
	except:
		print traceback.print_exc(file=sys.stdout)
	
if __name__ == "__main__":
	main()