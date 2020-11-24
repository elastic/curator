FROM python:3.7-alpine3.7 as builder

RUN apk --no-cache upgrade && apk --no-cache add build-base tar musl-utils openssl-dev
RUN pip3 install setuptools cx_Freeze

COPY . .
RUN ln -s /lib/libc.musl-x86_64.so.1 ldd
RUN ln -s /lib /lib64
RUN pip3 install -r requirements.txt
RUN python3 setup.py build_exe

FROM alpine:3.7
RUN apk --no-cache upgrade && apk --no-cache add openssl-dev
COPY --from=builder build/exe.linux-x86_64-3.7 /curator/

USER nobody:nobody
ENV LD_LIBRARY_PATH /curator/lib:$LD_LIBRARY_PATH
ENTRYPOINT ["/curator/curator"]

