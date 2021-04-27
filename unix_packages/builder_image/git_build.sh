#!/bin/bash

yum install -y perl-ExtUtils-MakeMaker
cd 
yum remove -y git
wget https://mirrors.edge.kernel.org/pub/software/scm/git/git-2.31.1.tar.gz
tar zxf git-2.31.1.tar.gz
cd git-2.31.1
make prefix=/usr all
make prefix=/usr install

cd ..
rm -rf git-2.31.*