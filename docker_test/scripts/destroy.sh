#!/bin/bash

if [ "x${1}" != "verbose" ]; then
  DEBUG=0
else
  DEBUG=1
fi

log_out () {
  # $1 is the log line
  if [ ${DEBUG} -eq 1 ]; then
    echo ${1}
  fi
}


# Stop running containers
echo
echo "Stopping all containers..."
RUNNING=$(docker ps | egrep 'curator.?-es-(remote|local)' | awk '{print $NF}')
for container in ${RUNNING}; do
  log_out "Stopping container ${container}..."
  log_out "$(docker stop ${container}) stopped."
done

# Remove existing containers
echo "Removing all containers..."
EXISTS=$(docker ps -a | egrep 'curator.?-es-(remote|local)' | awk '{print $NF}')
for container in ${EXISTS}; do
  log_out "Removing container ${container}..."
  log_out "$(docker rm -f ${container}) deleted."
done

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
else
  echo "WARNING: Unable to automatically empty bind mounted repo path."
  echo "Please manually empty the contents of the repo directory!"
fi

# Return to origin to be clean
cd $EXECPATH

echo "Cleanup complete."
