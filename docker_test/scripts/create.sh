#!/bin/bash

IMAGE=curator_estest
RUNNAME=curator7-es
LOCAL_NAME=${RUNNAME}-local
REMOTE_NAME=${RUNNAME}-remote
LOCAL_PORT=9200
REMOTE_PORT=9201
LOCAL_URL=http://127.0.0.1:${LOCAL_PORT}
REMOTE_URL=http://127.0.0.1:${REMOTE_PORT}

if [ "x$1" == "x" ]; then
  echo "Error! No Elasticsearch version provided."
  echo "VERSION must be in Semver format, e.g. X.Y.Z, 7.17.8"
  echo "USAGE: $0 VERSION"
  exit 1
fi

VERSION=$1

# Determine local IPs
OS=$(uname -a | awk '{print $1}')
if [[ "$OS" = "Linux" ]]; then
  IPLIST=$(ip -4 -o addr show scope global | grep -v docker |awk '{gsub(/\/.*/,"",$4); print $4}')
elif [[ "$OS" = "Darwin" ]]; then
  IPLIST=$(ifconfig | awk -F "[: ]+" '/inet / { if ($2 != "127.0.0.1") print $2 }')
else
  echo "Could not determine local IPs for assigning REMOTE_ES_SERVER env variable..."
  echo "Please manually determine your local non-loopback IP address and assign it to REMOTE_ES_SERVER"
  echo "e.g. REMOTE_ES_SERVER=http://A.B.C.D:${REMOTE_PORT} (be sure to use port ${REMOTE_PORT}!)"
  exit 0
fi

WHITELIST=""
for IP in $IPLIST; do
  if [ "x${WHITELIST}" == "x" ]; then
    WHITELIST="${IP}:${REMOTE_PORT}"
  else
    WHITELIST="${WHITELIST},${IP}:${REMOTE_PORT}"
  fi
done

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
  # Create a Dockerfile from the template
  cat Dockerfile.tmpl | sed -e "s/ES_VERSION/${VERSION}/" > Dockerfile
  docker build . -t ${IMAGE}:${VERSION}
fi

### Launch the containers (plural, in 7.x)
echo -en "\rStarting ${LOCAL_NAME} container... "
docker run -d --name ${LOCAL_NAME} -p ${LOCAL_PORT}:9200 -v ${REPOPATH}:/media \
-e "discovery.type=single-node" \
-e "cluster.name=local-cluster" \
-e "node.name=local" \
-e "xpack.monitoring.enabled=false" \
-e "path.repo=/media" \
-e "xpack.security.enabled=false" \
-e "reindex.remote.whitelist=${WHITELIST}" \
${IMAGE}:${VERSION}

echo -en "\rStarting ${REMOTE_NAME} container... "
docker run -d --name ${REMOTE_NAME} -p ${REMOTE_PORT}:9200 -v ${REPOPATH}:/media \
-e "discovery.type=single-node" \
-e "cluster.name=remote-cluster" \
-e "node.name=remote" \
-e "xpack.monitoring.enabled=false" \
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
echo "Please select one of these environment variables to prepend your 'pytest --cov=curator' run:"
echo

for IP in $IPLIST; do
  echo "REMOTE_ES_SERVER=\"$IP:${REMOTE_PORT}\""
done

echo
echo "Ready to test!"
