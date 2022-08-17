# syntax=docker/dockerfile:experimental
ARG VERSION=6.8.23
FROM elasticsearch:${VERSION}

COPY --chown=1000:0 eslauncher.sh /
RUN cp -R /usr/share/elasticsearch /esremote
RUN chown -R 1000:0 /esremote

#ENTRYPOINT exec /bin/bash
ENTRYPOINT exec /eslauncher.sh
