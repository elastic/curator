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
  export JAVA_HOME=$jhome
  elasticsearch/bin/elasticsearch $es_args > /tmp/elasticsearch.log &
  sleep 10
  curl http://localhost:9200 && echo "ES is up!" || cat /tmp/elasticsearch.log
}

if [[ "$ES_VERSION" == 5.* ]]; then
  setup_es https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-$ES_VERSION.tar.gz
else
  setup_es https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/$ES_VERSION/elasticsearch-$ES_VERSION.tar.gz
fi

java_home='/usr/lib/jvm/java-8-oracle'
if [[ "$ES_VERSION" == 5.* ]]; then
  start_es $java_home '-d -Edefault.path.repo=/'
else
  start_es $java_home '-d -Des.path.repo=/'
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
