#!/bin/bash
set -ex

setup_es() {
  download_url=$1
  curl -sL $download_url > elasticsearch.tar.gz
  mkdir elasticsearch
  tar -xzf elasticsearch.tar.gz --strip-components=1 -C ./elasticsearch/.

}

start_es() {
  jhome=$1
  es_args=$2
  export JAVA_HOME=$jhome
  elasticsearch/bin/elasticsearch $es_args > /tmp/elasticsearch.log &
  sleep 10
  curl http://localhost:9200 && echo "ES is up!" || cat /tmp/elasticsearch.log
}

setup_es https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/$ES_VERSION/elasticsearch-$ES_VERSION.tar.gz

java_home='/usr/lib/jvm/java-8-oracle'
if [[ "$ES_VERSION" == 5.* ]]; then
  start_es $java_home '-d -Edefault.path.repo=/'
else
  start_es $java_home '-d -Des.path.repo=/'
fi
python setup.py test
result=$(head -1 nosetests.xml | awk '{print $6 " " $7 " " $8}' | awk -F\> '{print $1}')
expected='errors="0" failures="0" skip="0"'
echo "Result = $result"
if [[ "$result" != "$expected" ]]; then
  exit 1
fi
