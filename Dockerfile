# Docker Definition for ElasticSearch Curator

FROM python:2.7.15-slim
MAINTAINER Christian R. Vozar <christian@rogueethic.com>

RUN pip install --quiet elasticsearch-curator

ENTRYPOINT [ "/usr/local/bin/curator" ]
