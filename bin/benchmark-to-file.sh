#!/bin/bash

git checkout "$2"

IPFS_LOGGING=critical go test -benchtime=3s -run=NONE -bench=. ./... | tee "$1"
