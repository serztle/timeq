#!/bin/bash

set -euo pipefail


usage() {
    echo "$0 [--help|--slow]"
    echo
    echo "Run testsuite."
    echo
    echo "--slow    Run also tests tagged as slow."
    echo
    exit 0
}

TAG_ARGUMENT=

while [[ $# -gt 0 ]]; do
  case $1 in
    --slow)
      TAG_ARGUMENT="--tags=slow"
      shift # past argument
      ;;
    -*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      usage
      ;;
  esac
done

# For the coverage we need to figure out a list of all packages
# (this enables coverage of tests that test code in other packages)
ALL_PACKAGES=$( \
    find -iname '*.go' -exec dirname {} \; | \
    tr -d '.' | \
    sort | \
    uniq  | \
    xargs -n1 printf 'github.com/sahib/timeq%s\n' | \
    paste -sd ',' \
)

gotestsum -- \
    ./... "${TAG_ARGUMENT}" \
    -race \
    -coverprofile=cover.out \
    -covermode=atomic \
    -coverpkg "${ALL_PACKAGES}"
