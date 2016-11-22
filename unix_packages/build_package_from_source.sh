#!/bin/bash

BASEPATH=$(pwd)
PKG_TARGET=/curator_packages
WORKDIR=/tmp/curator
CX_VER="5.0"
CX_FILE="/curator_source/unix_packages/cx_freeze-${CX_VER}.dev.tar.gz"
CX_PATH="anthony_tuininga-cx_freeze-71554144c9cc"
INPUT_TYPE=python
CATEGORY=python
VENDOR=Elastic
MAINTAINER="'Elastic Developers <info@elastic.co>'"
C_POST_INSTALL=${WORKDIR}/es_curator_after_install.sh
C_PRE_REMOVE=${WORKDIR}/es_curator_before_removal.sh
C_POST_REMOVE=${WORKDIR}/es_curator_after_removal.sh
C_PRE_UPGRADE=${WORKDIR}/es_curator_before_upgrade.sh
C_POST_UPGRADE=${WORKDIR}/es_curator_after_upgrade.sh

# Build our own package pre/post scripts
sudo rm -rf ${WORKDIR} /opt/elasticsearch-curator
mkdir -p ${WORKDIR}

for file in ${C_POST_INSTALL} ${C_PRE_REMOVE} ${C_POST_REMOVE}; do
  echo '#!/bin/bash' > ${file}
  echo >> ${file}
  chmod +x ${file}
done

echo "ln -s /opt/elasticsearch-curator/curator /usr/bin/curator" >> ${C_POST_INSTALL}
echo "ln -s /opt/elasticsearch-curator/curator_cli /usr/bin/curator_cli" >> ${C_POST_INSTALL}
echo "ln -s /opt/elasticsearch-curator/es_repo_mgr /usr/bin/es_repo_mgr" >> ${C_POST_INSTALL}
echo "ln -s /opt/elasticsearch-curator/curator /usr/bin/curator" >> ${C_POST_UPGRADE}
echo "ln -s /opt/elasticsearch-curator/curator_cli /usr/bin/curator_cli" >> ${C_POST_UPGRADE}
echo "ln -s /opt/elasticsearch-curator/es_repo_mgr /usr/bin/es_repo_mgr" >> ${C_POST_UPGRADE}
echo "rm -f /usr/bin/curator" >> ${C_PRE_REMOVE}
echo "rm -f /usr/bin/curator_cli" >> ${C_PRE_REMOVE}
echo "rm -f /usr/bin/es_repo_mgr" >> ${C_PRE_REMOVE}
echo "rm -f /usr/bin/curator" >> ${C_PRE_UPGRADE}
echo "rm -f /usr/bin/curator_cli" >> ${C_PRE_UPGRADE}
echo "rm -f /usr/bin/es_repo_mgr" >> ${C_PRE_UPGRADE}
echo 'if [ -d "/opt/elasticsearch-curator" ]; then' >> ${C_POST_REMOVE}
echo '  rm -rf /opt/elasticsearch-curator' >> ${C_POST_REMOVE}
echo 'fi' >> ${C_POST_REMOVE}

ID=$(grep ^ID\= /etc/*release | awk -F\= '{print $2}' | tr -d \")
VERSION_ID=$(grep ^VERSION_ID\= /etc/*release | awk -F\= '{print $2}' | tr -d \")
if [ "${ID}x" == "x" ]; then
  ID=$(cat /etc/*release | grep -v LSB | uniq | awk '{print $1}' | tr "[:upper:]" "[:lower:]" )
  VERSION_ID=$(cat /etc/*release | grep -v LSB | uniq | awk '{print $3}' | awk -F\. '{print $1}')
fi

# build
if [ "${1}x" == "x" ]; then
  echo "Must provide version number (can be arbitrary)"
  exit 1
else
  cd ..
  SOURCE_DIR=$(pwd)
fi

case "$ID" in
  ubuntu|debian)
  	PKGTYPE=deb
  	PLATFORM=debian
    PACKAGEDIR="${PKG_TARGET}/${1}/${PLATFORM}"
	;;
  centos|rhel)
  	PKGTYPE=rpm
    PLATFORM=centos
	case "$VERSION_ID" in
	  6|7)
      sudo rm -f /etc/yum.repos.d/puppetlabs-pc1.repo
      sudo yum -y update
		;;
 	  *) echo "unknown system version: ${VERSION_ID}"; exit 1;;
	esac
  PACKAGEDIR="${PKG_TARGET}/${1}/${PLATFORM}/${VERSION_ID}"
	;;
  *) echo "unknown system type: ${ID}"; exit 1;;
esac

HAS_PY3=$(which python3.5)
if [ "${HAS_PY3}x" == "x" ]; then
  cd /tmp
  wget https://www.python.org/ftp/python/3.5.2/Python-3.5.2.tgz
  tar zxf Python-3.5.2.tgz
  cd Python-3.5.2
  ./configure --prefix=/usr/local
  sudo make altinstall
  cd /usr/lib
  sudo ln -s /usr/local/lib/libpython3.5m.a libpython3.5.a
  cd ${WORKDIR}
fi

PYVER=3.5
PIPBIN=/usr/local/bin/pip3.5
PYBIN=/usr/local/bin/python3.5

if [ "${CX_VER}" != "$(${PIPBIN} list | grep cx | awk '{print $2}' | tr -d '()')" ]; then
  cd ${WORKDIR}
  rm -rf ${CX_PATH}
  tar zxf ${CX_FILE}
  cd ${CX_PATH}
  ${PIPBIN} install -U --user .
  cd ${WORKDIR}
fi

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

cd $SOURCE_DIR

mkdir -p ${PACKAGEDIR}
${PIPBIN} install -U --user setuptools
${PIPBIN} install -U --user requests_aws4auth
${PIPBIN} install -U --user -r requirements.txt
${PYBIN} setup.py build_exe
sudo mv build/exe.linux-x86_64-${PYVER} /opt/elasticsearch-curator

sudo chown -R root:root /opt/elasticsearch-curator
cd $WORKDIR
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
 --before-upgrade ${C_PRE_UPGRADE} \
 --after-upgrade ${C_POST_UPGRADE} \
 --provides elasticsearch-curator \
 --conflicts python-elasticsearch-curator \
 --conflicts python3-elasticsearch-curator \
/opt/elasticsearch-curator

mv ${WORKDIR}/*.${PKGTYPE} ${PACKAGEDIR}

rm ${C_POST_INSTALL} ${C_PRE_REMOVE} ${C_POST_REMOVE} ${C_PRE_UPGRADE} ${C_POST_UPGRADE}
# go back to where we started
cd ${BASEPATH}
