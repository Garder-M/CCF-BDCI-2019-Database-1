#!/bin/bash

#
# Run BDCI19 executable
#
# We grant the program root permission, cause it may:
#   - do some system configurations
#   - test whether a file is in page cache (which requires the file is writable by the program since Linux kernel 5.0+)
#   - decide to drop some page caches (which requries root permission)
# In real-world applications, programs should drop this dangerous privilege after initialization.
#
exec sudo "$(dirname "$0")/BDCI19" "$@"
