#!/bin/bash
set -ex

# There's at least 1 expected, skipped test, only with 5.0.0-alpha4 right now
expected_skips=1

setup_es() {
  download_url=$1
  curl -sL $download_url > elasticsearch.tar.gz
  mkdir elasticsearch
  tar -xzf elasticsearch.tar.gz --strip-components=1 -C ./elasticsearch/.
}

start_es() {
  jhome=$1
  es_args=$2
  es_port=$3
  es_cluster=$4
  export JAVA_HOME=$jhome
  elasticsearch/bin/elasticsearch $es_args > /tmp/$es_cluster.log &
  sleep 20
  curl http://127.0.0.1:$es_port && echo "$es_cluster Elasticsearch is up!" || cat /tmp/$es_cluster.log ./elasticsearch/logs/$es_cluster.log  
  # curl http://127.0.0.1:$es_port && echo "ES is up!" || cat /tmp/$es_cluster.log ./elasticsearch/logs/$es_cluster.log
}

start_es6() {
  jhome=$1
  es_args=$2
  path_env=$3
  es_port=$4
  es_cluster=$5
  export JAVA_HOME=$jhome
  ES_PATH_CONF=$path_env elasticsearch/bin/elasticsearch $es_args > /tmp/$es_cluster.log &
  sleep 20
  curl http://127.0.0.1:$es_port && echo "$es_cluster Elasticsearch is up!" || cat /tmp/$es_cluster.log ./elasticsearch/logs/$es_cluster.log
  # curl http://127.0.0.1:$es_port && echo "ES is up!" || cat /tmp/$es_cluster.log ./elasticsearch/logs/$es_cluster.log
}

start_es7() {
  es_args=$1
  path_env=$2
  es_port=$3
  es_cluster=$4
  ES_PATH_CONF=$path_env elasticsearch/bin/elasticsearch $es_args > /tmp/$es_cluster.log &
  sleep 20
  curl http://127.0.0.1:$es_port && echo "$es_cluster Elasticsearch is up!" || cat /tmp/$es_cluster.log ./elasticsearch/logs/$es_cluster.log
}

common_node_settings() {
  major=$1
  minor=$2
  port=$3
  clustername=$4
  file=$5
  echo 'network.host: 127.0.0.1' > $file
  echo "http.port: ${port}" >> $file
  echo "cluster.name: ${clustername}" >> $file
  echo "node.name: ${clustername}" >> $file
  echo 'node.max_local_storage_nodes: 2' >> $file
  if [[ $major -lt 7 ]]; then
    echo "discovery.zen.ping.unicast.hosts: [\"127.0.0.1:${port}\"]" >> $file
  else
    transport=$(($port+100))
    echo "transport.port: ${transport}" >> $file
    echo "discovery.seed_hosts: [\"localhost:${transport}\"]" >> $file
    echo "discovery.type: single-node" >> $file
  fi
  if [[ $major -ge 6 ]] && [[ $minor -ge 3 ]]; then
    echo 'xpack.monitoring.enabled: false' >> $file
    echo 'node.ml: false' >> $file
    echo 'xpack.security.enabled: false' >> $file
    echo 'xpack.watcher.enabled: false' >> $file
  fi
}

setup_es https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-$ES_VERSION.tar.gz

java_home='/usr/lib/jvm/java-8-openjdk-amd64/jre'

## Get major and minor version numbers
MAJORVER=$(echo $ES_VERSION | awk -F\. '{print $1}')
MINORVER=$(echo $ES_VERSION | awk -F\. '{print $2}')

### Build local cluster config (since 5.4 removed most flags)
LC=elasticsearch/localcluster
mkdir -p $LC
cp elasticsearch/config/log4j2.properties $LC
cp elasticsearch/config/jvm.options $LC
common_node_settings $MAJORVER $MINORVER 9200 "local" "$LC/elasticsearch.yml"
echo 'path.repo: /' >> $LC/elasticsearch.yml
echo 'reindex.remote.whitelist: localhost:9201' >> $LC/elasticsearch.yml


### Build remote cluster config (since 5.4 removed most flags)
RC=elasticsearch/remotecluster
mkdir -p $RC
cp elasticsearch/config/log4j2.properties $RC
cp elasticsearch/config/jvm.options $RC
common_node_settings $MAJORVER $MINORVER 9201 remote "$RC/elasticsearch.yml"

if [[ $MAJORVER -lt 6 ]]; then
  start_es $java_home "-d -Epath.conf=$LC" 9200 "local"
  start_es $java_home "-d -Epath.conf=$RC" 9201 "remote"
elif [[ $MARJORVER -eq 6 ]]; then
  start_es6 $java_home " " "$LC" 9200 "local"
  start_es6 $java_home " " "$RC" 9201 "remote"
else 
  start_es7 " " "$LC" 9200 "local"
  start_es7 " " "$RC" 9201 "remote"
fi

python setup.py test
result=$(head -1 nosetests.xml | awk '{print $6 " " $7 " " $8}' | awk -F\> '{print $1}' | tr -d '"')
echo "Result = $result"
errors=$(echo $result | awk '{print $1}' | awk -F\= '{print $2}')
failures=$(echo $result | awk '{print $2}' | awk -F\= '{print $2}')
skips=$(echo $result | awk '{print $3}' | awk -F\= '{print $2}')
if [[ $errors -gt 0 ]]; then
  exit 1
elif [[ $failures -gt 0 ]]; then
  exit 1
elif [[ $skips -gt $expected_skips ]]; then
  exit 1
fi
