#!/usr/bin/env python3
import shutil
import certifi
from platform import machine, system, python_version
MAJOR, MINOR = tuple(python_version().split('.')[:-1])
SYSTEM = system().lower()
BUILD = f'build/exe.{system().lower()}-{machine()}-{MAJOR}.{MINOR}'
CERT = certifi.where()
TARGET = 'curator_build'

# First copy the cert to BUILD
shutil.copy(CERT, BUILD)

# Then rename the path of BUILD itself
shutil.move(BUILD, TARGET)
