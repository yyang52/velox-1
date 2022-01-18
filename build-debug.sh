#!/usr/bin/env bash
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

SCRIPT_ROOT=`dirname $(readlink -f ${BASH_SOURCE[0]})`

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
OMNISCI_DIR=$SCRIPT_ROOT/../../omniscidb
export C_INCLUDE_PATH=$OMNISCI_DIR:$OMNISCI_DIR/build-$BUILD_TYPE:$C_INCLUDE_PATH
export CPLUS_INCLUDE_PATH=$OMNISCI_DIR:$OMNISCI_DIR/build-$BUILD_TYPE:$CPLUS_INCLUDE_PATH
export LIBRARY_PATH=$OMNISCI_DIR/build-$BUILD_TYPE/Embedded:$LIBRARY_PATH
export LD_LIBRARY_PATH=$OMNISCI_DIR/build-$BUILD_TYPE/Embedded:$LD_LIBRARY_PATH
export EXTRA_CMAKE_OPTIONS="-O0"

rm -rf build-$BUILD_TYPE
mkdir build-$BUILD_TYPE
cd build-$BUILD_TYPE

cmake  \
    -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
    -DEXEC_WITH_OMNISCI=ON \
    ..
    
make -j ${CPU_COUNT:-`nproc`} || make -j $((${CPU_COUNT:-`nproc`}/2)) || make -j $((${CPU_COUNT:-`nproc`}/4))
