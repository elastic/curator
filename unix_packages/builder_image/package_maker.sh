#!/bin/bash

# Build our own package pre/post scripts
rm -rf ${WORKDIR} ${VENVDIR} ${SRCDIR} /opt/elasticsearch-curator
mkdir -p ${WORKDIR}

#/usr/local/bin/python?.? --version
#Python 3.9.4
RAWPYVER=$(/usr/local/bin/python?.? --version | awk '{print $2}')
PYVER=$(echo $RAWPYVER | awk -F\. '{print $1"."$2}')
MINOR=$(echo $RAWPYVER | awk -F\. '{print $3}')

for file in ${C_POST_INSTALL} ${C_PRE_REMOVE} ${C_POST_REMOVE}; do
  echo '#!/bin/bash' > ${file}
  echo >> ${file}
  chmod +x ${file}
done

for binary_name in curator curator_cli es_repo_mgr; do
  for script_target in ${C_POST_INSTALL} ${C_POST_UPGRADE}; do 
    echo "echo '#!/bin/bash' > /usr/bin/${binary_name}" >> ${script_target}
    echo "echo >> /usr/bin/${binary_name}" >> ${script_target}
    echo "echo -n LD_LIBRARY_PATH=/opt/elasticsearch-curator/lib /opt/elasticsearch-curator/${binary_name} >> /usr/bin/${binary_name}" >> ${script_target}
    echo "echo ' \"\$@\"' >> /usr/bin/${binary_name}" >> ${script_target}
    echo "chmod +x /usr/bin/${binary_name}" >> ${script_target}
  done
  for script_target in ${C_PRE_REMOVE} ${C_PRE_UPGRADE}; do 
    echo "rm -f /usr/bin/${binary_name}" >> ${script_target}
    echo "rm -f /etc/ld.so.conf.d/elasticsearch-curator.conf" >> ${script_target}
    echo "ldconfig" >> ${script_target}
  done
done

echo 'if [ -d "/opt/elasticsearch-curator" ]; then' >> ${C_POST_REMOVE}
echo '  rm -rf /opt/elasticsearch-curator' >> ${C_POST_REMOVE}
echo 'fi' >> ${C_POST_REMOVE}

# build
if [ "${1}x" == "x" ]; then
  echo "Must provide version number (can be arbitrary)"
  exit 1
else
  mkdir -p ${SRCDIR}
  cd ${SRCDIR}
  git clone https://github.com/elastic/curator.git
  cd curator
  git fetch --all --tags
  git checkout tags/v${1} -b mybuild_${1}
  RESPONSE=$?
  if [ $RESPONSE -ne 0 ]; then
    RESPONSE=0
    echo "tags/v${1} not found!"
    echo "Checking for tags/V${1}..."
    git checkout tags/V${1} -b mybuild_${1}
    RESPONSE=$?
  fi
  if [ $RESPONSE -ne 0 ]; then
    echo "Unable to checkout remote tag v${1}"  
    exit 1
  fi
  GIT_PATH=$(pwd)
fi

if [ -e "/usr/local/rvm/scripts/rvm" ]; then
  source /usr/local/rvm/scripts/rvm
fi


# Build virtualenv
mkdir -p ${VENVDIR}/${PYVER}.${MINOR}
cd ${VENVDIR}
/usr/local/bin/virtualenv ${PYVER}.${MINOR}
source ${VENVDIR}/${PYVER}.${MINOR}/bin/activate
pip install cx_freeze patchelf-wrapper

# Install pre-requisites
cd ${GIT_PATH}
pip install -r requirements.txt
python setup.py build_exe

mv build/exe.linux-x86_64-${PYVER} /opt/elasticsearch-curator
chown -R root:root /opt/elasticsearch-curator
cd $WORKDIR

for pkgtype in rpm deb; do
  fpm \
   -s dir \
   -t ${pkgtype} \
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
   --before-upgrade ${C_PRE_UPGRADE} \
   --after-upgrade ${C_POST_UPGRADE} \
   --provides elasticsearch-curator \
   --conflicts python-elasticsearch-curator \
   --conflicts python3-elasticsearch-curator \
  /opt/elasticsearch-curator
  mv ${WORKDIR}/*.${pkgtype} ${PKG_TARGET}
done

deactivate

rm ${C_POST_INSTALL} ${C_PRE_REMOVE} ${C_POST_REMOVE} ${C_PRE_UPGRADE} ${C_POST_UPGRADE}
rm -rf ${SRCDIR} ${VENVDIR}
