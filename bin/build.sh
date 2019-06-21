#!/bin/bash
set -eo pipefail

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

build_all