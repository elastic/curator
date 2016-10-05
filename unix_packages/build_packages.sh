#!/bin/bash

BASEPATH=$(pwd)
PKG_TARGET=/curator_packages
WORKDIR=/tmp/curator
PATCHFILE=cx_freeze.setup.py.patch
CX_VER="4.3.4"
CX_FILE="${CX_VER}.tar.gz"
CX_PATH="anthony_tuininga-cx_freeze-f4b85046f841"
INPUT_TYPE=python
CATEGORY=python
VENDOR=Elastic
MAINTAINER="'Elastic Developers <info@elastic.co>'"
PY_BIN="'/usr/bin/python'"
PY_EASY_INSTALL="'/usr/bin/easy_install'"
PY_BIN3="'/usr/bin/python3'"
PY_EASY_INSTALL3="'/usr/bin/easy_install3'"
VAF=${WORKDIR}/voluptuous_after_install.sh
C_POST_INSTALL=${WORKDIR}/es_curator_after_install.sh
C_PRE_REMOVE=${WORKDIR}/es_curator_before_removal.sh
C_POST_REMOVE=${WORKDIR}/es_curator_after_removal.sh
EXECUTOR=${WORKDIR}/execute_me.sh

# Build our own package pre/post scripts
sudo rm -rf ${WORKDIR} /opt/elasticsearch-curator
mkdir -p ${WORKDIR}
# Put the patchfile here before we do any cd
cp ${PATCHFILE} ${WORKDIR}
for file in ${VAF} ${C_POST_INSTALL} ${C_PRE_REMOVE} ${C_POST_REMOVE} ${EXECUTOR}; do
  echo '#!/bin/bash' > ${file}
  echo >> ${file}
  chmod +x ${file}
done

echo "ln -s /opt/elasticsearch-curator/curator /usr/bin/curator" >> ${C_POST_INSTALL}
echo "rm /usr/bin/curator" >> ${C_PRE_REMOVE}
echo 'if [ -d "/opt/elasticsearch-curator" ]; then' >> ${C_POST_REMOVE}
echo '  rmdir /opt/elasticsearch-curator' >> ${C_POST_REMOVE}
echo 'fi' >> ${C_POST_REMOVE}

ID=$(grep ^ID\= /etc/*release | awk -F\= '{print $2}' | tr -d \")
VERSION_ID=$(grep ^VERSION_ID\= /etc/*release | awk -F\= '{print $2}' | tr -d \")
if [ "${ID}x" == "x" ]; then
  ID=$(cat /etc/*release | grep -v LSB | uniq | awk '{print $1}' | tr "[:upper:]" "[:lower:]" )
  VERSION_ID=$(cat /etc/*release | grep -v LSB | uniq | awk '{print $3}' | awk -F\. '{print $1}')
fi

# build
if [ "${1}x" == "x" ]; then
  echo "Must provide version number"
  exit 1
else
  FILE="v${1}.tar.gz"
  cd ${WORKDIR}
  wget https://github.com/elastic/curator/archive/${FILE}
fi

case "$ID" in
  ubuntu|debian)
	PKGTYPE=deb
	PLATFORM=debian
        PACKAGEDIR="${PKG_TARGET}/${1}/${PLATFORM}"
	PYVER=2.7
	DEPS="-d 'python:any << 3.0' -d 'python:any >= 2.7'"
	DEPS3="-d 'python3:any << 4.0' -d 'python3:any >= 3.4'"
	echo 'chmod o+r /usr/local/lib/python*/dist-packages/voluptuous-*.egg-info/*' >> ${VAF}
	# Install patched version of cx_freeze
	if [ "${CX_VER}" != "$(pip list | grep cx | awk '{print $2}' | tr -d '()')" ]; then
	  rm -rf ${CX_PATH} ${CX_FILE}
	  wget https://bitbucket.org/anthony_tuininga/cx_freeze/get/${CX_FILE}
	  tar zxf ${CX_FILE}
	  cd ${CX_PATH}
	  patch < ${WORKDIR}/cx_freeze.setup.py.patch
	  pip install -U --user .
	  cd ${WORKDIR}
	fi
	;;
  centos|rhel)
	PKGTYPE=rpm
        PLATFORM=centos
	case "$VERSION_ID" in
	  6)
		PYVER=2.6
		DEPS="-d 'python(abi) <= 2.7'"
		echo 'chmod o+r /usr/lib/python2.6/site-packages/voluptuous-*-py2.6.egg-info/*' >> ${VAF}
		;;
          7)
		PYVER=2.7
		DEPS="-d 'python(abi) >= 2.7'"
		echo 'chmod o+r /usr/lib/python2.7/site-packages/voluptuous-*-py2.7.egg-info/*' >> ${VAF}
		;;
 	  *) echo "unknown system version: ${VERSION_ID}"; exit 1;;
	esac
        PACKAGEDIR="${PKG_TARGET}/${1}/${PLATFORM}/${VERSION_ID}"
	pip install -U --user cx-Freeze
	;;
  *) echo "unknown system type: ${ID}"; exit 1;;
esac

if [ -e "/home/vagrant/.rvm/scripts/rvm" ]; then
  source /home/vagrant/.rvm/scripts/rvm
fi
HAS_FPM=$(which fpm)
if [ "${HAS_FPM}x" == "x" ]; then
  gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3
  curl -sSL https://get.rvm.io | bash -s stable
  source /home/vagrant/.rvm/scripts/rvm
  rvm install ruby
  gem install fpm
fi

tar zxf ${FILE}


mkdir -p ${PACKAGEDIR}
cd curator-${1}
pip install -U --user setuptools
pip install -U --user requests_aws4auth
pip install -U --user certifi
pip install -U --user -r requirements.txt
python setup.py build_exe
sudo mv build/exe.linux-x86_64-${PYVER} /opt/elasticsearch-curator
sudo chown -R root:root /opt/elasticsearch-curator
cd ..
fpm \
 -s dir \
 -t ${PKGTYPE} \
 -n elasticsearch-curator \
 -v ${1} \
 --vendor ${VENDOR} \
 --maintainer "${MAINTAINER}" \
 --license 'Apache-2.0' \
 --category tools \
 --description 'Have indices in Elasticsearch? This is the tool for you!\n\nLike a museum curator manages the exhibits and collections on display, \nElasticsearch Curator helps you curate, or manage your indices.' \
 --after-install ${C_POST_INSTALL} \
 --before-remove ${C_PRE_REMOVE} \
 --after-remove ${C_POST_REMOVE} \
 --provides elasticsearch-curator \
 --conflicts python-elasticsearch-curator \
 --conflicts python3-elasticsearch-curator \
/opt/elasticsearch-curator
mv *.${PKGTYPE} ${PACKAGEDIR}

if [ "${PLATFORM}" == "debian" ]; then
  loop=2
else
  loop=1
fi
for ((i=0;i<loop;i++)); do
  if [ $i -eq 0 ]; then
    DEPENDENCIES="${DEPS}"
    MY_PY=${PY_BIN}
    MY_EASY=${PY_EASY_INSTALL}
    PY_PREFIX=python
  else
    DEPENDENCIES="${DEPS3}"
    MY_PY=${PY_BIN3}
    MY_EASY=${PY_EASY_INSTALL3}
    PY_PREFIX=python3
  fi
  cd ${PACKAGEDIR}

  # We echo these out to file to guarantee proper argument recognition
  # There are problems with quote encapsulation that aren't solvable in a
  # single pass, unfortunately.
  echo fpm \
    -s ${INPUT_TYPE} -t ${PKGTYPE} --category ${CATEGORY} \
    --vendor ${VENDOR} --maintainer ${MAINTAINER} ${DEPENDENCIES} \
    --python-bin ${MY_PY} --python-easyinstall ${MY_EASY} \
    --python-package-name-prefix ${PY_PREFIX} \
    --license "'MPL-2.0'" \
    --description "'Elastic build of certifi module'" \
    certifi >> ${EXECUTOR}

  echo fpm \
    -s ${INPUT_TYPE} -t ${PKGTYPE} --category ${CATEGORY} \
    --vendor ${VENDOR} --maintainer ${MAINTAINER} ${DEPENDENCIES} \
    --python-bin ${MY_PY} --python-easyinstall ${MY_EASY} \
    --python-package-name-prefix ${PY_PREFIX} \
    --license "'BSD'" \
    --description "'Elastic build of click module'" \
    click >> ${EXECUTOR}

  echo fpm \
    -s ${INPUT_TYPE} -t ${PKGTYPE} --category ${CATEGORY} \
    --vendor ${VENDOR} --maintainer ${MAINTAINER} ${DEPENDENCIES} \
    --python-bin ${MY_PY} --python-easyinstall ${MY_EASY} \
    --python-package-name-prefix ${PY_PREFIX} \
    --license "'Apache-2.0'" \
    --description "'Elastic build of elasticsearch module'" \
    elasticsearch >> ${EXECUTOR}

  echo fpm \
    -s ${INPUT_TYPE} -t ${PKGTYPE} --category ${CATEGORY} \
    --vendor ${VENDOR} --maintainer ${MAINTAINER} ${DEPENDENCIES} \
    --python-bin ${MY_PY} --python-easyinstall ${MY_EASY} \
    --python-package-name-prefix ${PY_PREFIX} \
    --license "'PSF|ZPL'" \
    --description "'Elastic build of setuptools module'" \
    setuptools >> ${EXECUTOR}

  echo fpm \
    -s ${INPUT_TYPE} -t ${PKGTYPE} --category ${CATEGORY} \
    --vendor ${VENDOR} --maintainer ${MAINTAINER} ${DEPENDENCIES} \
    --python-bin ${MY_PY} --python-easyinstall ${MY_EASY} \
    --python-package-name-prefix ${PY_PREFIX} \
    --description "'Elastic build of PyYAML module'" \
    pyyaml >> ${EXECUTOR}

  echo fpm \
    -s ${INPUT_TYPE} -t ${PKGTYPE} --category ${CATEGORY} \
    --vendor ${VENDOR} --maintainer ${MAINTAINER} ${DEPENDENCIES} \
    --python-bin ${MY_PY} --python-easyinstall ${MY_EASY} \
    --after-install ${VAF} \
    --python-package-name-prefix ${PY_PREFIX} \
    --description "'Elastic build of voluptuous module'" \
    voluptuous >> ${EXECUTOR}

  echo fpm \
    -s ${INPUT_TYPE} -t ${PKGTYPE} --category ${CATEGORY} \
    --vendor ${VENDOR} --maintainer ${MAINTAINER} ${DEPENDENCIES} \
    --python-bin ${MY_PY} --python-easyinstall ${MY_EASY} \
    --python-package-name-prefix ${PY_PREFIX} \
    --license "'MIT'" \
    --description "'Elastic build of Urllib3 module'" \
    urllib3 >> ${EXECUTOR}

  echo cd ${WORKDIR}/curator-${1} >> ${EXECUTOR}

  echo fpm \
    -s ${INPUT_TYPE} -t ${PKGTYPE} --category ${CATEGORY} \
    --vendor ${VENDOR} --maintainer ${MAINTAINER} ${DEPENDENCIES} \
    --python-bin ${MY_PY} --python-easyinstall ${MY_EASY} \
    --python-package-name-prefix ${PY_PREFIX} \
    --conflicts elasticsearch-curator \
    --license "'Apache-2.0'" \
    --description "'Have indices in Elasticsearch? This is the tool for you!\n\nLike a museum curator manages the exhibits and collections on display, \nElasticsearch Curator helps you curate, or manage your indices.'" \
    setup.py  >> ${EXECUTOR}

  echo cd ${WORKDIR} >> ${EXECUTOR}
done

# Execute the file now that builds the packages.
${EXECUTOR}
# Copy the built packages from the curator-${1} directory to the package dir
mv ${WORKDIR}/curator-${1}/*.${PKGTYPE} ${PACKAGEDIR}
# cleanup
rm ${VAF} ${C_POST_INSTALL} ${C_PRE_REMOVE} ${C_POST_REMOVE} ${EXECUTOR}
# go back to where we started
cd ${BASEPATH}
