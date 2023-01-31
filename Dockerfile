# syntax=docker/dockerfile:1
FROM python:3.11.1-alpine3.17 as builder

# Add the community repo for access to patchelf binary package
RUN echo 'https://dl-cdn.alpinelinux.org/alpine/v3.17/community/' >> /etc/apk/repositories
RUN apk --no-cache upgrade && apk --no-cache add build-base tar musl-utils openssl-dev patchelf
# patchelf-wrapper is necessary now for cx_Freeze, but not for Curator itself.
RUN pip3 install setuptools cx_Freeze patchelf-wrapper

COPY . .
RUN ln -s /lib/libc.musl-x86_64.so.1 ldd
RUN ln -s /lib /lib64
RUN pip3 install .
RUN python3 setup.py build_exe
# This will add the cacert.pem from certifi to the default location Curator will look
RUN <<eot
#!/usr/bin/env python3
import os
import shutil
import certifi
DEST = 'build/exe.linux-x86_64-3.11'
CERT = certifi.where()
shutil.copy(CERT, DEST)
eot

FROM alpine:3.17
RUN apk --no-cache upgrade && apk --no-cache add openssl-dev expat
COPY --from=builder build/exe.linux-x86_64-3.11 /curator/
RUN mkdir /.curator

USER nobody:nobody
ENV LD_LIBRARY_PATH /curator/lib:$LD_LIBRARY_PATH
ENTRYPOINT ["/curator/curator"]
