#!/bin/bash
#
# this comes from: https://github.com/python-zk/kazoo/blob/master/ensure-zookeeper-env.sh
#
# Copyright 2015 Pinterest, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

HERE=`pwd`
ZOO_BASE_DIR="$HERE/zookeeper"
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION:-3.5.0-alpha}
ZOOKEEPER_PATH="$ZOO_BASE_DIR/$ZOOKEEPER_VERSION"
ZOO_MIRROR_URL="http://apache.osuosl.org/"


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

# Yield execution

$*