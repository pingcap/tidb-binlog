#!/usr/bin/env bash

echo "generate go code..."

GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf

for FILE in pump; do
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=plugins=grpc:. proto/${FILE}.proto
sed -i.bak -E 's/import _ \"gogoproto\"//g' proto/${FILE}.pb.go
sed -i.bak -E 's/import fmt \"fmt\"//g' proto/${FILE}.pb.go
rm -f proto/${FILE}.pb.go.bak
goimports -w proto/${FILE}.pb.go
done
