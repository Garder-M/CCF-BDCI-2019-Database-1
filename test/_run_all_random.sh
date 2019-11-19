#!/bin/bash

cd "$(dirname "$0")"

for file in random_seed_*; do
    if [ -x "$file" ]; then
        echo "======== $file ========"
	"./$file"
    fi
done

