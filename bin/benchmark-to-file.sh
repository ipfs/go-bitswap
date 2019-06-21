#!/bin/bash
output="$1"
branch="$2"

git checkout "$2"

IPFS_LOGGING=critical go test -benchtime=3s -run=NONE -bench=. ./... | tee "$1"
