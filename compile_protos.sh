#!/bin/bash

# This script compiles the .proto files into Python gRPC code.

# Exit immediately if a command exits with a non-zero status.
set -e

# The directory where the generated files will be placed.
# It needs to be a package, so we'll create an __init__.py file.
GEN_DIR="common/generated"
touch "${GEN_DIR}/__init__.py"

# Run the gRPC Python protoc plugin.
# This generates the _pb2.py (messages) and _pb2_grpc.py (stubs and servers) files.
python -m grpc_tools.protoc \
       -I./protos \
       --python_out=${GEN_DIR} \
       --grpc_python_out=${GEN_DIR} \
       ./protos/orchestrator.proto

echo "Protobuf files compiled successfully."
