#!/bin/bash

. ./common.sh

if ! check_protoc_version; then
	exit 1
fi

# install protobuf-codegen 2.X if it's missing.
if ! cargo install --list|grep "protobuf-codegen"|grep -E "v2\."; then
    echo "missing protobuf-codegen 2.X, trying to download/install it"
    cargo install protobuf-codegen --vers ">=2.0,<3" || exit 1
fi

if ! cmd_exists grpc_rust_plugin; then
    echo "missing grpc_rust_plugin, trying to download/install it"
    cargo install grpcio-compiler || exit 1
fi

push proto
echo "generate rust code..."
gogo_protobuf_url=github.com/gogo/protobuf
GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf
GO_INSTALL='go install'

echo "install gogoproto code/generator ..."
${GO_INSTALL} ${gogo_protobuf_url}/proto
${GO_INSTALL} ${gogo_protobuf_url}/protoc-gen-gofast
${GO_INSTALL} ${gogo_protobuf_url}/gogoproto

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

protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf:../include --rust_out ../src *.proto || exit $?
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf:../include --grpc_out ../src --plugin=protoc-gen-grpc=`which grpc_rust_plugin` *.proto || exit $?
pop

push src
LIB_RS=`mktemp`
rm -f lib.rs
cat <<EOF > ${LIB_RS}
extern crate futures;
extern crate grpcio;
extern crate protobuf;
extern crate raft;

use raft::eraftpb;

EOF
for file in `ls *.rs`
    do
    base_name=$(basename $file ".rs")
    echo "pub mod $base_name;" >> ${LIB_RS}
done
mv ${LIB_RS} lib.rs
pop

# Use the old way to read protobuf enums.
# TODO: Remove this once stepancheg/rust-protobuf#233 is resolved.
for f in src/*; do
python <<EOF
import re
with open("$f") as reader:
    src = reader.read()

res = re.sub('::protobuf::rt::read_proto3_enum_with_unknown_fields_into\(([^,]+), ([^,]+), &mut ([^,]+), [^\)]+\)\?', 'if \\\\1 == ::protobuf::wire_format::WireTypeVarint {\\\\3 = \\\\2.read_enum()?;} else { return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type)); }', src)

with open("$f", "w") as writer:
    writer.write(res)
EOF
done

cargo build
