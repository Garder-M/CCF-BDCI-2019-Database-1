#!/bin/bash

set -v
set -e

cd "$(dirname "$0")"
shopt -s dotglob

mkdir -p build
cd build
rm -rf *

cmake .. -DCMAKE_BUILD_TYPE=Release -DMAKE_FASTEST=ON

make -j

mv BDCI19 ../

cd ..
rm -rf build

