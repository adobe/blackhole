#!/bin/sh
set -e -x
# --- Switch to script's base directory.
cd $(dirname ${0})

BVER="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

# echo ${target} ${target%%/*} ${target#*/} ${${target#*/}%%-*} ${${target#*/}#*-}
# build/linux-amd64 build linux-amd64 linux amd64


rm -fr build
for target in linux-amd64 darwin-amd64 windows-amd64 windows-386
do
	mkdir -p build/${target}
	export GOOS=${target%%-*}
        export GOARCH=${target#*-}
	go build -o build/${target} -ldflags "-X main.buildTS=${BVER}" ./...
done
