#!/usr/bin/env bash

echo "generate go code..."

GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf

function gen()
{
    protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=plugins=grpc:. proto/$1.proto
    sed -i.bak -E 's/import _ \"gogoproto\"//g' proto/$1.pb.go
    sed -i.bak -E 's/import fmt \"fmt\"//g' proto/$1.pb.go
    rm -f proto/$1.pb.go.bak
    goimports -w proto/$1.pb.go
}

if [ $# -eq 0 ]; then
    for FULLNAME in `ls proto/*.proto`; do
        FILE=`basename ${FULLNAME%.*}`
        gen $FILE
    done
else
    until [ $# -eq 0 ]
    do
    gen $1
    shift
    done
fi

