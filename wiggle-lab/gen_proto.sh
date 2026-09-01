#!/usr/bin/env bash
# Compile the wiggle protos into Python gRPC stubs under wigglelab/pb/.
# Idempotent: safe to re-run whenever proto/src/main/proto changes.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
proto_src="$here/../proto/src/main/proto"
out="$here/wigglelab/pb"

[ -d "$proto_src" ] || { echo "proto source not found at $proto_src" >&2; exit 1; }
mkdir -p "$out"
touch "$out/__init__.py"

python -m grpc_tools.protoc \
  -I "$proto_src" \
  --python_out="$out" --grpc_python_out="$out" \
  "$proto_src/wiggle.proto" "$proto_src/coordinator.proto"

# grpc_tools emits top-level imports (`import wiggle_pb2`); rewrite them to package-relative
# so the stubs import correctly from within wigglelab.pb.
python - "$out" <<'PY'
import re, sys, pathlib
out = pathlib.Path(sys.argv[1])
for f in out.glob("*_pb2*.py"):
    text = f.read_text()
    text = re.sub(r'^import (wiggle_pb2|coordinator_pb2)( as \w+)?$',
                  lambda m: f'from . import {m.group(1)}{m.group(2) or ""}',
                  text, flags=re.M)
    f.write_text(text)
PY

echo "generated Python stubs in $out"
