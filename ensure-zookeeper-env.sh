#!/bin/bash

set -x -e

HERE=`pwd`
ZOO_BASE_DIR="$HERE/bin/"
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION:-3.4.6}
ZOOKEEPER_PATH="$ZOO_BASE_DIR/zookeeper-$ZOOKEEPER_VERSION"
ZOO_MIRROR_URL="http://apache.osuosl.org/"


function download_zookeeper(){
    mkdir -p $ZOO_BASE_DIR
    cd $ZOO_BASE_DIR
	curl -C - $ZOO_MIRROR_URL/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz | tar -zx
	chmod a+x $ZOOKEEPER_PATH/bin/zkServer.sh
}

if [ ! -d "$ZOOKEEPER_PATH" ]; then
    download_zookeeper
fi

export ZOOKEEPER_PATH
cd $HERE

# Yield execution to venv command

$*

