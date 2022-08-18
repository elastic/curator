#!/bin/bash

VERSION=8.0.1
IMAGE=curator_estest
RUNNAME=curator8-es
LOCAL_NAME=${RUNNAME}-local
REMOTE_NAME=${RUNNAME}-remote
LOCAL_URL=http://127.0.0.1:9200
REMOTE_URL=http://127.0.0.1:9201


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
  echo "Docker image ${IMAGE}:${VERSION} not found. Building from Dockerfile..."
  cd $SCRIPTPATH
  docker build . -t ${IMAGE}:${VERSION}
fi

### Launch the containers (plural, in 8.x)
echo -en "\rStarting ${LOCAL_NAME} container... "
docker run -d --name ${LOCAL_NAME} -p 9200:9200 -v ${REPOPATH}:/media \
-e "discovery.type=single-node" \
-e "cluster.name=local-cluster" \
-e "node.name=local" \
-e "xpack.monitoring.templates.enabled=false" \
-e "path.repo=/media" \
-e "xpack.security.enabled=false" \
${IMAGE}:${VERSION}

echo -en "\rStarting ${REMOTE_NAME} container... "
docker run -d --name ${REMOTE_NAME} -p 9201:9200 -v ${REPOPATH}:/media \
-e "discovery.type=single-node" \
-e "cluster.name=remote-cluster" \
-e "node.name=remote" \
-e "xpack.monitoring.templates.enabled=false" \
-e "path.repo=/media" \
-e "xpack.security.enabled=false" \
${IMAGE}:${VERSION}

### Check to make sure the ES instances are up and running
echo
echo "Waiting for Elasticsearch instances to become available..."
echo
EXPECTED=200
for URL in $LOCAL_URL $REMOTE_URL; do
  if [[ "$URL" = "$LOCAL_URL" ]]; then
    NODE="${LOCAL_NAME} instance"
  else
    NODE="${REMOTE_NAME} instance"
  fi
  ACTUAL=0
  while [ $ACTUAL -ne $EXPECTED ]; do
    ACTUAL=$(curl -o /dev/null -s -w "%{http_code}\n" $URL)
    echo -en "\rHTTP status code for $NODE is: $ACTUAL"
    if [ $EXPECTED -eq $ACTUAL ]; then
      echo " --- $NODE is ready!"
    fi
    sleep 1
  done
done

# Done
echo
echo "Creation complete. ${LOCAL_NAME} and ${REMOTE_NAME} containers are up using image ${IMAGE}:${VERSION}"

echo
echo "Please select one of these environment variables to prepend your 'python setup.py test' run:"
echo

# Determine local IPs
OS=$(uname -a | awk '{print $1}')
if [[ "$OS" = "Linux" ]]; then
  IPLIST=$(ip -4 -o addr show scope global | grep -v docker |awk '{gsub(/\/.*/,"",$4); print $4}')
elif [[ "$OS" = "Darwin" ]]; then
  IPLIST=$(ifconfig | awk -F "[: ]+" '/inet / { if ($2 != "127.0.0.1") print $2 }')
else
  echo "Could not determine local IPs for assigning REMOTE_ES_SERVER env variable..."
  echo "Please manually determine your local non-loopback IP address and assign it to REMOTE_ES_SERVER"
  echo "e.g. REMOTE_ES_SERVER=A.B.C.D:9201 (be sure to use port 9201!)"
  exit 0
fi

for IP in $IPLIST; do
  echo "REMOTE_ES_SERVER=$IP:9201"
done

echo
echo "Ready to test!"
