# syntax=docker/dockerfile:experimental
ARG VERSION=7.17.5
FROM elasticsearch:${VERSION}

COPY --chown=1000:0 eslauncher.sh /
COPY --chown=1000:0 small.options /usr/share/elasticsearch/config/jvm.options.d
RUN cp -R /usr/share/elasticsearch /esremote
RUN chown -R 1000:0 /esremote

#ENTRYPOINT exec /bin/bash
ENTRYPOINT exec /eslauncher.sh
