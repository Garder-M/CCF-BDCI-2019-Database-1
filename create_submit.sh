#!/bin/bash

set -v
set -e

cd "$(dirname "$0")"

rm -rf submit.tar.gz submit
mkdir submit
cd submit

# Copy source code
cp -a ../sourcecode ../run.sh ../compile.sh ../README.md ../CMakeLists.txt ./

# Compile and generate binary
./compile.sh

# Create compressed file
cd ..
tar -cf submit.tar.gz submit

rm -rf submit
