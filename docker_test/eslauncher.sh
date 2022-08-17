#!/bin/bash
set -e

# Files created by Elasticsearch should always be group writable too
umask 0002

run_as_other_user_if_needed() {
  if [[ "$(id -u)" == "0" ]]; then
    # If running as root, drop to specified UID and run command
    exec chroot --userspec=1000 / "${@}"
  else
    # Either we are running in Openshift with random uid and are a member of the root group
    # or with a custom --user
    exec "${@}"
  fi
}

export ES_JAVA_OPTS="-Des.cgroups.hierarchy.override=/ $ES_JAVA_OPTS"

run_as_other_user_if_needed /esremote/bin/elasticsearch -Ecluster.name=curator6-remote -Expack.watcher.enabled=false -Enode.ml=false -Ediscovery.type=single-node -Enode.name=remote -Expack.monitoring.enabled=false -Ebootstrap.memory_lock=true -Epath.repo=/media -Epath.data=/esremote/data -Expack.security.enabled=false -Enode.max_local_storage_nodes=2 -Ehttp.port=9201 -Etransport.port=9301 &

run_as_other_user_if_needed /usr/share/elasticsearch/bin/elasticsearch -Ecluster.name=curator6-local -Expack.watcher.enabled=false -Enode.ml=false -Ediscovery.type=single-node -Enode.name=local -Expack.monitoring.enabled=false -Ebootstrap.memory_lock=true -Epath.repo=/media -Expack.security.enabled=false -Enode.max_local_storage_nodes=2 -Ehttp.port=9200 -Etransport.port=9300 -Ereindex.remote.whitelist=localhost:9201
