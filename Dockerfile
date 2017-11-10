# Docker Definition for ElasticSearch Curator

FROM python:2.7.8-slim
MAINTAINER Christian R. Vozar <christian@rogueethic.com>

RUN pip install --quiet elasticsearch-curator

ADD ./run_continuously.sh /run_continuously.sh

ENTRYPOINT [ "/run_continuously.sh" ]
