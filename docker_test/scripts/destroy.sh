#!/bin/bash

# First, stop and remove the docker container
docker stop curator8-es-local curator8-es-remote
docker rm curator8-es-local curator8-es-remote

### Now begins the repo cleanup phase

# Save original execution path
EXECPATH=$(pwd)

# Extract the path for the script
SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# Navigate to the script, regardless of whether we were there
cd $SCRIPTPATH

# Remove the created Dockerfile
rm -f Dockerfile

# Go up one directory
cd ..

# Find out what the last part of this directory is called
UPONE=$(pwd | awk -F\/ '{print $NF}')

if [[ "$UPONE" = "docker_test" ]]; then
  rm -rf $(pwd)/repo/*
  rm -rf $(pwd)/curatortestenv
else
  echo "WARNING: Unable to automatically empty bind mounted repo path."
  echo "Please manually empty the contents of the repo directory!"
fi

# Return to origin to be clean
cd $EXECPATH

echo "Cleanup complete."

