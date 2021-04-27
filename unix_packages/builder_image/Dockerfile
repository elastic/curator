## If needing Debian style, uncomment this, and comment the CentOS ones
#FROM ubuntu:latest as builder

FROM centos:6 as builder

# Can change these
ENV PYVER=3.9
ENV PYPATCH=4
ENV OPENSSL_VER=1.1.1k

# Don't change these
ENV PKG_TARGET=/curator_packages
ENV WORKDIR=/tmp/curator
ENV VENVDIR=/opt/python
ENV SRCDIR=/opt/src
ENV INPUT_TYPE=python
ENV CATEGORY=python
ENV VENDOR=Elastic
ENV MAINTAINER="'Elastic Developers <info@elastic.co>'"
ENV C_POST_INSTALL=${WORKDIR}/es_curator_after_install.sh
ENV C_PRE_REMOVE=${WORKDIR}/es_curator_before_removal.sh
ENV C_POST_REMOVE=${WORKDIR}/es_curator_after_removal.sh
ENV C_PRE_UPGRADE=${WORKDIR}/es_curator_before_upgrade.sh
ENV C_POST_UPGRADE=${WORKDIR}/es_curator_after_upgrade.sh

## If running Debian-style, uncomment these and comment the CentOS ones
#RUN apt update
#RUN apt dist-upgrade -y
#RUN ln -fs /usr/share/zoneinfo/America/Denver /etc/localtime
#RUN DEBIAN_FRONTEND=noninteractive apt install -y tzdata
#RUN apt install -y build-essential git ca-certificates zlib1g zlib1g-dev libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev libgdbm-dev libc6-dev libbz2-dev dirmngr curl liblzma-dev libffi-dev gnupg2 rpm

## CentOS 6 covers all bases with its ANCIENT libc
# This replaces the base yum repo definition since v6 is EOL now
# It won't work without it
COPY CentOS-Base.repo /etc/yum.repos.d
RUN yum -y groupinstall "Development Tools"
RUN yum -y install zlib-devel bzip2-devel curl-devel expat-devel gettext-devel sqlite-devel openssl-devel curl wget ncurses-devel readline-devel gdbm-devel xz-devel libffi-devel libuuid-devel which

# Build patchelf
COPY patchelf_build.sh /
RUN /patchelf_build.sh

# Build newer Git 
COPY git_build.sh /
RUN /git_build.sh

# Build OpenSSL
RUN curl -O https://www.openssl.org/source/openssl-${OPENSSL_VER}.tar.gz
RUN tar zxf openssl-${OPENSSL_VER}.tar.gz
RUN cd openssl-${OPENSSL_VER}; ./config --prefix=/usr/local/openssl --openssldir=/usr/local/openssl shared zlib; make; make install
RUN echo "# /etc/profile.d/openssl.sh" > /etc/profile.d/openssl.sh
RUN echo "pathmunge /usr/local/openssl/bin" >> /etc/profile.d/openssl.sh
RUN echo "# /etc/ld.so/conf.d/openssl-${OPENSSL_VER}.conf" > /etc/ld.so.conf.d/openssl-${OPENSSL_VER}.conf
RUN echo "/usr/local/openssl/lib" >> /etc/ld.so.conf.d/openssl-${OPENSSL_VER}.conf
RUN ldconfig
RUN rm -rf openssl-${OPENSSL_VER} openssl-${OPENSSL_VER}.tar.gz

# Build Python
RUN curl -O https://www.python.org/ftp/python/${PYVER}.${PYPATCH}/Python-${PYVER}.${PYPATCH}.tgz
RUN tar zxf Python-${PYVER}.${PYPATCH}.tgz
RUN cd Python-${PYVER}.${PYPATCH}; ./configure --prefix=/usr/local --with-openssl=/usr/local/openssl --enable-optimizations --enable-shared; make -j3 altinstall
RUN echo "# /etc/ld.so.conf.d/python${PYVER}.${PYPATCH}.conf" > /etc/ld.so.conf.d/python${PYVER}.${PYPATCH}.conf
RUN echo "/usr/local/lib" >> /etc/ld.so.conf.d/python${PYVER}.${PYPATCH}.conf
RUN echo "/usr/local/lib/python${PYVER}" >> /etc/ld.so.conf.d/python${PYVER}.${PYPATCH}.conf
RUN ldconfig
RUN /usr/local/bin/pip${PYVER} install virtualenv
RUN rm -rf Python-${PYVER}.${PYPATCH}.tgz Python-${PYVER}.${PYPATCH}

# Install RVM
COPY ruby_build.sh /
COPY rpm.erb.patched /
RUN /ruby_build.sh

# Cleanup after all this installation
RUN yum clean all

COPY package_maker.sh /
RUN mkdir /curator_packages
