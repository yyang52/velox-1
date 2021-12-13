#!/bin/sh

SCRIPT_ROOT="$( cd -- "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 ; pwd -P )"
#SCRIPT_ROOT=`dirname $(readlink -f ${BASH_SOURCE[0]})`

PREFIX=/usr/local
export PATH=$PREFIX/bin:$PATH
export PATH=$PREFIX/go/bin:$PATH
export LD_LIBRARY_PATH=$PREFIX/lib64:$PREFIX/lib:$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:$PKG_CONFIG_PATH
export C_INCLUDE_PATH=$PREFIX/include:$C_INCLUDE_PATH
export CPLUS_INCLUDE_PATH=$PREFIX/include:$CPLUS_INCLUDE_PATH
export LIBRARY_PATH=$PREFIX/lib:$PREFIX/lib64:$LIBRARY_PATH
export CMAKE_PREFIX_PATH=$PREFIX:$CMAKE_PREFIX_PATH
export VULKAN_SDK=$PREFIX
export VK_LAYER_PATH=$PREFIX/etc/vulkan/explicit_layer.d
export GOROOT=$PREFIX/go

BUILD_TYPE=Debug
OMNISCI_DIR=/workspace/omniscidb
VELOX_DIR=/workspace/presto_cpp/velox
export C_INCLUDE_PATH=$OMNISCI_DIR/ThirdParty/rapidjson/:$OMNISCI_DIR:$VELOX_DIR:$C_INCLUDE_PATH
export CPLUS_INCLUDE_PATH=$OMNISCI_DIR/Catalog/os/:$OMNISCI_DIR/build-$BUILD_TYPE:$OMNISCI_DIR/Distributed/os/:$OMNISCI_DIR/Shared/:$OMNISCI_DIR/ThirdParty/rapidjson/:$OMNISCI_DIR:$VELOX_DIR:$CPLUS_INCLUDE_PATH
export LIBRARY_PATH=$OMNISCI_DIR/ThirdParty/rapidjson/rapidjson/:$OMNISCI_DIR/build-$BUILD_TYPE/Embedded:$OMNISCI_DIR/build-$BUILD_TYPE/Cider:$LIBRARY_PATH
export LD_LIBRARY_PATH=$OMNISCI_DIR/build-$BUILD_TYPE/Embedded:$OMNISCI_DIR/build-$BUILD_TYPE/Cider:$LD_LIBRARY_PATH
