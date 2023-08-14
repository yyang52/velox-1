#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
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

# Minimal setup for Ubuntu 20.04.
set -eufx -o pipefail

BUILD_PROTOBUF=ON

ENABLE_HDFS=OFF

ENABLE_S3=OFF

for arg in "$@"; do
  case $arg in
  --build_protobuf=*)
    BUILD_PROTOBUF=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function process_setup_ubuntu {
  # make this function Reentrantly
  # git checkout scripts/setup-ubuntu.sh
  sed -i '/libprotobuf-dev/d' scripts/setup-ubuntu.sh
  sed -i '/protobuf-compiler/d' scripts/setup-ubuntu.sh
  if [ $ENABLE_HDFS == "ON" ]; then
    sed -i '/^function install_fmt.*/i function install_libhdfs3 {\n  github_checkout oap-project/libhdfs3 master \n cmake_install\n}\n' scripts/setup-ubuntu.sh
    sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_libhdfs3' scripts/setup-ubuntu.sh
    sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  yasm \\' scripts/setup-ubuntu.sh
  fi
  if [ $BUILD_PROTOBUF == "ON" ]; then
    sed -i '/^function install_fmt.*/i function install_protobuf {\n  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz\n  tar -xzf protobuf-all-21.4.tar.gz\n  cd protobuf-21.4\n  ./configure  --prefix=/usr/local\n  make "-j$(nproc)"\n  sudo make install\n  sudo ldconfig\n}\n' scripts/setup-ubuntu.sh
    sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_protobuf' scripts/setup-ubuntu.sh
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    sed -i '/^function install_fmt.*/i function install_awssdk {\n  github_checkout aws/aws-sdk-cpp 1.9.379 --depth 1 --recurse-submodules\n  cmake_install -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management" \n} \n' scripts/setup-ubuntu.sh
    sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_awssdk' scripts/setup-ubuntu.sh
  fi
  sed -i 's/run_and_time install_conda/#run_and_time install_conda/' scripts/setup-ubuntu.sh
}

process_setup_ubuntu

echo "finish setup extra deps"