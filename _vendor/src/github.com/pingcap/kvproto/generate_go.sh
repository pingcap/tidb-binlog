#!/usr/bin/env bash

PROGRAM=$(basename "$0")

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi

GO_PREFIX_PATH=github.com/pingcap/kvproto/pkg

gogo_protobuf_url=github.com/gogo/protobuf
GOGO_ROOT=${GOPATH}/src/${gogo_protobuf_url}
GO_OUT_M=

cmd_exists () {
    which "$1" 1>/dev/null 2>&1
}

# download gogproto code and install its binary if it's missing
if ! cmd_exists protoc-gen-gofast || [ ! -e "$GOGO_ROOT" ]; then
    echo "gogoproto code/generator missing, try to download/install it"
    go get ${gogo_protobuf_url}/proto
    go get ${gogo_protobuf_url}/protoc-gen-gofast
    go get ${gogo_protobuf_url}/gogoproto
fi

# add the bin path of gogoproto generator into PATH if it's missing
if ! cmd_exists protoc-gen-gofast; then
    for path in $(echo "${GOPATH}" | sed -e 's/:/ /g'); do
        gogo_proto_bin="${path}/bin/protoc-gen-gofast"
        if [ -e "${gogo_proto_bin}" ]; then
            export PATH=$(dirname "${gogo_proto_bin}"):$PATH
            break
        fi
    done
fi

cd proto
for file in `ls *.proto`
    do
    base_name=$(basename $file ".proto")
    mkdir -p ../pkg/$base_name
    if [ -z $GO_OUT_M ]; then
        GO_OUT_M="M$file=$GO_PREFIX_PATH/$base_name"
    else
        GO_OUT_M="$GO_OUT_M,M$file=$GO_PREFIX_PATH/$base_name"
    fi
done

echo "generate go code..."
ret=0
for file in `ls *.proto`
    do
    base_name=$(basename $file ".proto")
    protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gofast_out=$GO_OUT_M:../pkg/$base_name $file || ret=$?
    cd ../pkg/$base_name
    sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
    sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
    sed -i.bak -E 's/import io \"io\"//g' *.pb.go
    sed -i.bak -E 's/import math \"math\"//g' *.pb.go
    rm -f *.bak
    goimports -w *.pb.go
    cd ../../proto
done
exit $ret
