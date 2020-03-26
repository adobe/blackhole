#!/bin/bash
# --- Switch to script's base directory.
cd $(dirname ${0})

for i in $(find cmd -name *.go | xargs -L1 dirname | sort -u)
do
    j=$(basename $i)
    if ! [[ $j == x* ]]; then
        echo github.com/adobe/blackhole/$i
    fi
done
