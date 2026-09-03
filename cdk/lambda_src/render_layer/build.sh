#!/bin/bash
# Rebuilds the vendored PDF layer. python/ is gitignored — 54MB of wheels
# isn't source, and this script is the reproducible way back to it.
#
# The --platform/--python-version flags are load-bearing, not decoration:
# this account's Lambdas run x86_64 Linux, and a Mac-native `pip install`
# grabs macOS binaries that import fine here and fail on Lambda.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf python
pip install \
  --platform manylinux2014_x86_64 \
  --python-version 3.13 \
  --only-binary=:all: \
  --target python \
  -r requirements.txt
echo "built $(du -sh python | cut -f1) into $(pwd)/python"
