#!/bin/bash
set -eo pipefail

display_and_run() {
    echo "***" "$@"
	eval "$(printf '%q ' "$@")"
}

# reset workdir to state from git (to remove possible rewritten dependencies)
export GO111MODULE=on
display_and_run go get golang.org/x/tools/cmd/benchcmp
display_and_run git reset --hard
git checkout -b after
git fetch origin master:refs/remotes/origin/before
git checkout remotes/origin/before
git checkout -b before
git checkout after
display_and_run BENCHMARK_SEED="$$" ./bin/benchmark-to-file.sh benchmark-before.txt before
git checkout after
display_and_run BENCHMARK_SEED="$$" ./bin/benchmark-to-file.sh benchmark-after.txt after
if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
display_and_run ./bin/diff-benchmarks.sh benchmark-before.txt benchmark-after.txt
fi
