#!/bin/bash

VERSION=7.11.2
IMAGE=curator_estest
RUNNAME=curator7-es


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
  REPOPATH=$(pwd)/repo
  # Nuke it from orbit, just to be sure
  rm -rf ${REPOPATH}
  mkdir -p ${REPOPATH}
else
  echo "Unable to correctly locate bind mount repo path. Exiting."
  exit 1
fi

# Check if the image has been built. If not, build it.
if [[ "$(docker images -q ${IMAGE}:${VERSION} 2> /dev/null)" == "" ]]; then
  cd $SCRIPTPATH
  docker build . -t ${IMAGE}:${VERSION}
fi

### Launch the container
docker run -d --name ${RUNNAME} -p 9200:9200 -p 9201:9201 -v ${REPOPATH}:/media ${IMAGE}:${VERSION}

### Check to make sure the ES instances are up and running
EXPECTED=200
for URL in http://127.0.0.1:9200 http://127.0.0.1:9201; do
  ACTUAL=0
  while [ $ACTUAL -ne $EXPECTED ]; do
    ACTUAL=$(curl -o /dev/null -s -w "%{http_code}\n" $URL)
    echo -en "\r$ACTUAL status code for $URL"
    if [ $EXPECTED -eq $ACTUAL ]; then
      echo; echo "$URL is up!"
    fi
    sleep 1
  done
done

# Done
echo "Creation complete. ${RUNNAME} is up using image ${IMAGE}:${VERSION}"
