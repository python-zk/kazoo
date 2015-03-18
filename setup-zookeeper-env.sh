#!/bin/bash

set -x -e

HERE=`pwd`
ZOO_TMP_DIR=$(mktemp -d /tmp/ZOO-TMP-XXXXX)
mkdir -p $ZOO_TMP_DIR/bin
mkdir -p $ZOO_TMP_DIR/log

ZOOKEEPER_STARTED=0
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION:-3.4.6}
ZOO_BASE_DIR="$PWD/bin/"
ZOOKEEPER_PATH="$ZOO_BASE_DIR/zookeeper-$ZOOKEEPER_VERSION"


function download_zookeeper(){
    mkdir -p $ZOO_BASE_DIR
    cd $ZOO_BASE_DIR && \
	curl -C - http://apache.osuosl.org/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz | tar -zx
	chmod a+x $ZOOKEEPER_PATH/bin/zkServer.sh
}


function stop_zookeeper_server(){
    $ZOO_TMP_DIR/bin/zkServer.sh stop
}


function clean_exit(){
    local error_code="$?"
    if [ $ZOOKEEPER_STARTED -eq 1 ]; then
        stop_zookeeper_server
    fi
    rm -rf ${ZOO_TMP_DIR}
    return $error_code
}


function start_zookeeper_server(){
    # Copy zookeeper scripts in temporary directory
    cp $ZOOKEEPER_PATH/bin/* $ZOO_TMP_DIR/bin

    # Copy zookeeper conf and set dataDir variable to the zookeeper temporary
    # directory
    cp "$ZOOKEEPER_PATH/conf/zoo_sample.cfg" "$ZOO_TMP_DIR/zoo.cfg"

    sed -i -r "s@(dataDir *= *).*@\1$ZOO_TMP_DIR@" $ZOO_TMP_DIR/zoo.cfg

    # Replace some variables by the zookeeper temporary directory
    sed -i -r "s@(ZOOCFGDIR *= *).*@\1$ZOO_TMP_DIR@" $ZOO_TMP_DIR/bin/zkEnv.sh

    sed -i -r "s@(ZOO_LOG_DIR *= *).*@\1$ZOO_TMP_DIR/log@" $ZOO_TMP_DIR/bin/zkEnv.sh

    $ZOO_TMP_DIR/bin/zkServer.sh start
}


if [ ! -d "$ZOOKEEPER_PATH" ]; then
    download_zookeeper
fi

trap "clean_exit" EXIT

start_zookeeper_server
if [ $? -eq 0 ]; then
    ZOOKEEPER_STARTED=1
fi

export ZOOKEEPER_PATH
cd $HERE

# Yield execution to venv command
$*

