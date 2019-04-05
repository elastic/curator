ARG BASE_VERSION=20190221_114834
ARG ROOT_IMAGE=580698825394.dkr.ecr.eu-central-1.amazonaws.com/base

FROM ${ROOT_IMAGE}:${BASE_VERSION}

WORKDIR /github.com/elastic/curator/

COPY . .

RUN apt-get update && apt-get install python3 python3-pip libssl-dev musl zlib1g-dev -y && apt-get clean

RUN pip3 install -r requirements.txt &&\
    pip3 install setuptools cx_Freeze==6.0b1 requests-aws4auth boto3

COPY . .
RUN ln -s /lib/libc.musl-x86_64.so.1 ldd
RUN ln -s /lib /lib64
RUN pip3 install -r requirements.txt
RUN python3 setup.py build_exe

FROM ${ROOT_IMAGE}:${BASE_VERSION}

RUN apt-get update && apt-get install wget -y && apt-get clean

COPY --from=0 build/exe.linux-x86_64-3.6 /curator/
USER nobody:nobody
ENTRYPOINT ["/curator/curator"]
