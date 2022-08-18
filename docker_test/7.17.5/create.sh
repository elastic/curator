#!/bin/bash

docker run -d --name curator7-es -p 9200:9200 -p 9201:9201 \
-v ~/WORK/curator/docker_test/repo:/media \
curator_estest:7.17.5
