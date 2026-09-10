#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "Generating Python gRPC stubs from proto/bsos.proto..."
python3 -m grpc_tools.protoc \
    -I"${REPO_ROOT}/proto" \
    --python_out="${SCRIPT_DIR}" \
    --grpc_python_out="${SCRIPT_DIR}" \
    "${REPO_ROOT}/proto/bsos.proto"

echo "Stubs generated successfully: bsos_pb2.py, bsos_pb2_grpc.py"
