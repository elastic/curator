#!/bin/bash

# First, stop and remove the docker container
docker stop curator7-es
docker rm curator7-es

### Now begins the repo cleanup phase

# Save original execution path
EXECPATH=$(pwd)

# Extract the path for the script
SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# Navigate to the script, regardless of whether we were there
cd $SCRIPTPATH

# Go up one directory
cd ..

# Find out what the last part of this directory is called
UPONE=$(pwd | awk -F\/ '{print $NF}')

if [[ "$UPONE" = "docker_test" ]]; then
  rm -rf $(pwd)/repo/*
else
  echo "WARNING: Unable to automatically empty bind mounted repo path."
  echo "Please manually empty the contents of the repo directory!"
fi

# Return to origin to be clean
cd $EXECPATH

echo "Cleanup complete."

