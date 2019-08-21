#!/bin/bash

while true
do
  echo "Starting curator"
  /usr/local/bin/curator --config $CONFIG_FILE $ACTION_FILE
  echo "finished cleaning indexes..sleeping for 1h"
  sleep 1h
done
