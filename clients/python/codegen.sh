#!/usr/bin/env bash
#
# Regenerates the gRPC stubs in wiggle/_proto/ from the canonical proto (../../proto).
# Run this whenever proto/src/main/proto/wiggle.proto changes.
#
#   pip install grpcio-tools
#   clients/python/codegen.sh
set -euo pipefail
cd "$(dirname "$0")"

PROTO_DIR="../../proto/src/main/proto"
OUT="wiggle/_proto"

python3 -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT" \
    --grpc_python_out="$OUT" \
    "$PROTO_DIR/wiggle.proto"

# the grpc stub imports 'wiggle_pb2' absolutely; make it relative so it works as a package
perl -pi -e 's/^import wiggle_pb2 as/from . import wiggle_pb2 as/' "$OUT/wiggle_pb2_grpc.py"
: > "$OUT/__init__.py"
echo "regenerated stubs in $OUT"
