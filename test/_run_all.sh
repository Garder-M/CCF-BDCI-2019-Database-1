#!/bin/bash

cd "$(dirname "$0")"

for file in *; do
    if [ -f "$file" ] && [ -x "$file" ]; then
        case "$file" in
        _*):
            # Just ignore _xxx
            ;;
        *):
            echo "======== $file ========"
            "./$file"
            ;;
        esac
    fi
done

