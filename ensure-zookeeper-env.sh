#!/bin/bash

set -e

HERE=`pwd`
ZOO_BASE_DIR="$HERE/zookeeper"
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION:-3.4.10}
ZOOKEEPER_PATH="$ZOO_BASE_DIR/$ZOOKEEPER_VERSION"
ZOO_MIRROR_URL="http://archive.apache.org/dist"


function download_zookeeper(){
    mkdir -p $ZOO_BASE_DIR
    cd $ZOO_BASE_DIR
    curl --silent -C - $ZOO_MIRROR_URL/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz | tar -zx
    mv zookeeper-$ZOOKEEPER_VERSION $ZOOKEEPER_VERSION
    chmod a+x $ZOOKEEPER_PATH/bin/zkServer.sh
}

if [ ! -d "$ZOOKEEPER_PATH" ]; then
    download_zookeeper
    echo "Downloaded zookeeper $ZOOKEEPER_VERSION to $ZOOKEEPER_PATH"
else
    echo "Already downloaded zookeeper $ZOOKEEPER_VERSION to $ZOOKEEPER_PATH"
fi

export ZOOKEEPER_PATH
cd $HERE

# Yield execution to venv command

$*

