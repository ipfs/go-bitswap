#!/bin/bash
set -eo pipefail

display_and_run() {
    echo "***" "$@"
	eval $(printf '%q ' "$@")
}

# reset workdir to state from git (to remove possible rewritten dependencies)
display_and_run git reset --hard

# Environment
echo "*** Setting up test environment"
if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
    if [[ "$TRAVIS_SUDO" == true ]]; then
        # Ensure that IPv6 is enabled.
        # While this is unsupported by TravisCI, it still works for localhost.
        sudo sysctl -w net.ipv6.conf.lo.disable_ipv6=0
        sudo sysctl -w net.ipv6.conf.default.disable_ipv6=0
        sudo sysctl -w net.ipv6.conf.all.disable_ipv6=0
    fi
else
    # OSX has a default file limit of 256, for some tests we need a
    # maximum of 8192.
    ulimit -Sn 8192
fi

list_buildable() {
    go list -f '{{if (len .GoFiles)}}{{.ImportPath}} {{if .Module}}{{.Module.Dir}}{{else}}{{.Dir}}{{end}}{{end}}' ./... | grep -v /vendor/
}

build_all() {
    # Make sure everything can compile since some package may not have tests
    # Note: that "go build ./..." will fail if some packages have only
    #   tests (will get "no buildable Go source files" error) so we
    #   have to do this the hard way.
    list_buildable | while read -r pkg dir; do
        echo '*** go build' "$pkg"
        buildmode=archive
        if [[ "$(go list -f '{{.Name}}')" == "main" ]]; then
            # plugin works even when a "main" function is missing.
            buildmode=plugin
        fi
        ( cd "$dir"; go build -buildmode=$buildmode -o /dev/null "$pkg")
    done
}

export GO111MODULE=on
display_and_run go get golang.org/x/tools/cmd/benchcmp
build_all
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