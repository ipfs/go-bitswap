#!/bin/bash

parse() {
    sed -n \
        -e 's/ *\t */\t/g' \
        -e '/^Benchmark/p' |
        awk 'BEGIN{print "{\"results\": ["} {print "  {\"name\": \"",$1,"\", \"time\": ",$3," },"} END{print "]}"}' OFS="" ORS=" "|
        sed -e 's/, ]/ ]/g' |
        jq '.results[] | {name: .name, time: .time }'

}

benchcmp "$1" "$2"

echo ""
echo "Result:"

{
    parse < "$1"
    parse < "$2"
} | jq -e -r -s 'group_by(.name)[] | {name: .[0].name, speedup: (.[0].time / .[1].time)} | select(.speedup < 0.75) | "\(.name)\t\(.speedup)x"'

if [[ $? -ne 4 ]]; then
    echo ""
    echo "FAIL"
    exit 1
else
    echo "PASS"
fi
