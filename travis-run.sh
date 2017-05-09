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

if [[ "$ES_VERSION" == 5.* ]]; then
  setup_es https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-$ES_VERSION.tar.gz
else
  setup_es https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/$ES_VERSION/elasticsearch-$ES_VERSION.tar.gz
fi

start_es() {
  jhome=$1
  es_port=$2
  es_cluster=$3
  export JAVA_HOME=$jhome
  elasticsearch/bin/elasticsearch $es_args > /tmp/$es_cluster.log &
  sleep 20
  curl http://127.0.0.1:$es_port && echo "$es_cluster Elasticsearch is up!" || cat /tmp/$es_cluster.log ./elasticsearch/logs/$es_cluster.log  
}

java_home='/usr/lib/jvm/java-8-oracle'

### Build local cluster config (since 5.4 removed most flags)
LC=elasticsearch/config
mkdir -p $LC
echo 'network.host: 127.0.0.1' > $LC/elasticsearch.yml
echo 'http.port: 9200' >> $LC/elasticsearch.yml
echo 'cluster.name: local' >> $LC/elasticsearch.yml
echo 'discovery.zen.ping.unicast.hosts: ["127.0.0.1:9200"]' >> $LC/elasticsearch.yml
echo 'path.repo: /' >> $LC/elasticsearch.yml

start_es $java_home 9200 "local"

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
