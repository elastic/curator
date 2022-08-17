#!/bin/bash

docker run -d --name curator6-es -p 9200:9200 -p 9201:9201 \
-v ~/WORK/curator/docker_test/repo:/media \
curator_estest:6.2.23
