#!/bin/bash

BASEPATH=$(pwd)
PKG_TARGET=/curator_packages
WORKDIR=/tmp/curator
PYVER=3.6
MINOR=3
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

remove_python() {
  sudo rm -f /usr/local/lib/libpython${1}m.a
  sudo rm -f /usr/local/lib/pkgconfig/python-${1}.pc
  sudo rm -rf /usr/local/lib/python${1}
  sudo rm -f /usr/lib/libpython${1}.a
  sudo rm -rf /usr/local/include/python${1}m
  cd /usr/local/bin
  sudo rm -f 2to3-${1} easy_install-${1} idle${1} pip${1} pydoc${1} python${1} python${1}m python${1}m-config pyvenv-${1}
  cd -
}

build_python() {
  cd /tmp
  wget -c https://www.python.org/ftp/python/${1}/Python-${1}.tgz
  tar zxf Python-${1}.tgz
  cd Python-${1}
  ./configure --prefix=/usr/local
  sudo make altinstall
  sudo ln -s /usr/local/lib/libpython${1}m.a /usr/lib/libpython${1}.a
  cd -
}

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
  cd $(dirname $0)/..
  SOURCE_DIR=$(pwd)
fi

case "$ID" in
  ubuntu|debian)
  	PKGTYPE=deb
  	PLATFORM=debian
    case "$VERSION_ID" in
      1404|1604|8) PACKAGEDIR="${PKG_TARGET}/${1}/${PLATFORM}";;
      9)           PACKAGEDIR="${PKG_TARGET}/${1}/${PLATFORM}${VERSION_ID}";;
      *)           PACKAGEDIR="${PKG_TARGET}/${1}/${PLATFORM}";;
    esac
    sudo apt update -y
    sudo apt install -y openssl zlib1g zlib1g-dev libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev dirmngr curl
	;;
  centos|rhel)
  	PKGTYPE=rpm
    PLATFORM=centos
	case "$VERSION_ID" in
	  6|7)
      sudo rm -f /etc/yum.repos.d/puppetlabs-pc1.repo
      sudo yum -y update
      sudo yum install -y openssl
		;;
 	  *) echo "unknown system version: ${VERSION_ID}"; exit 1;;
	esac
  PACKAGEDIR="${PKG_TARGET}/${1}/${PLATFORM}/${VERSION_ID}"
	;;
  *) echo "unknown system type: ${ID}"; exit 1;;
esac

HAS_PY3=$(which python${PYVER})
if [ "${HAS_PY3}x" == "x" ]; then
  build_python ${PYVER}.${MINOR}
fi

FOUNDVER=$(python${PYVER} --version | awk '{print $2}')
if [ "${FOUNDVER}" != "${PYVER}.${MINOR}" ]; then
  remove_python $(echo ${FOUNDVER} | awk -F\. '{print $1"."$2}')
  build_python ${PYVER}.${MINOR}
fi

PIPBIN=/usr/local/bin/pip${PYVER}
PYBIN=/usr/local/bin/python${PYVER}

if [ -e "${HOME}/.rvm/scripts/rvm" ]; then
  source ${HOME}/.rvm/scripts/rvm
fi
HAS_FPM=$(which fpm)
if [ "${HAS_FPM}x" == "x" ]; then
  gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3
  curl -sSL https://get.rvm.io | bash -s stable
  source ${HOME}/.rvm/scripts/rvm
  rvm install ruby
  gem install fpm
fi

${PIPBIN} install -U --user setuptools
${PIPBIN} install -U --user requests_aws4auth
${PIPBIN} install -U --user boto3
${PIPBIN} install -U --user cx_freeze

cd $SOURCE_DIR

mkdir -p ${PACKAGEDIR}
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
