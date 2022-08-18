# syntax=docker/dockerfile:experimental
ARG VERSION=8.3.3
FROM elasticsearch:${VERSION}

COPY --chown=1000:0 small.options /usr/share/elasticsearch/config/jvm.options.d
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["eswrapper"]
