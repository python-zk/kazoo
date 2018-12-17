#!/bin/bash

set -e

HERE=`pwd`
ZOO_BASE_DIR="$HERE/zookeeper"
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION:-3.4.13}
ZOOKEEPER_PATH="$ZOO_BASE_DIR/$ZOOKEEPER_VERSION"
ZOOKEEPER_PREFIX=${ZOOKEEPER_PREFIX}
ZOOKEEPER_SUFFIX=${ZOOKEEPER_SUFFIX}
ZOO_MIRROR_URL="http://archive.apache.org/dist"


function download_zookeeper(){
    mkdir -p $ZOO_BASE_DIR
    cd $ZOO_BASE_DIR
    curl --silent -C - $ZOO_MIRROR_URL/zookeeper/zookeeper-$ZOOKEEPER_VERSION/${ZOOKEEPER_PREFIX}zookeeper-${ZOOKEEPER_VERSION}${ZOOKEEPER_SUFFIX}.tar.gz | tar -zx
    mv ${ZOOKEEPER_PREFIX}zookeeper-${ZOOKEEPER_VERSION}${ZOOKEEPER_SUFFIX} $ZOOKEEPER_VERSION
    chmod a+x $ZOOKEEPER_PATH/bin/zkServer.sh
}

if [ ! -d "$ZOOKEEPER_PATH" ]; then
    download_zookeeper
    echo "Downloaded zookeeper $ZOOKEEPER_VERSION to $ZOOKEEPER_PATH"
else
    echo "Already downloaded zookeeper $ZOOKEEPER_VERSION to $ZOOKEEPER_PATH"
fi

# Used as install_path when starting ZK
export ZOOKEEPER_PATH="${ZOOKEEPER_PATH}/${ZOOKEEPER_LIB}"
cd $HERE

# Yield execution to venv command

exec $*
