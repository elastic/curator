#!/bin/bash

wget http://people.centos.org/tru/devtools-2/devtools-2.repo -O /etc/yum.repos.d/devtools-2.repo
yum install -y devtoolset-2-gcc devtoolset-2-binutils
yum install -y devtoolset-2-gcc-c++ devtoolset-2-gcc-gfortran
scl enable devtoolset-2 bash
source /opt/rh/devtoolset-2/enable
cd 
git clone https://github.com/NixOS/patchelf.git
cd patchelf
./bootstrap.sh
./configure
make
make check
make install

# Cleanup after
cd
rm -rf patchelf
exit 0