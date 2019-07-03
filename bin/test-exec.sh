#!/bin/bash
if [[ "$TEST_PHASE" == "test" ]]; then
  bash <(curl -s https://raw.githubusercontent.com/ipfs/ci-helpers/master/travis-ci/run-standard-tests.sh)
else
  ./bin/run-benchmarks.sh
fi