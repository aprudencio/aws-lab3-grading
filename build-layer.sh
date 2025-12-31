#!/bin/bash
# Build script to prepare Lambda layer dependencies

set -e

LAYER_DIR="src/python_deps"
PYTHON_VERSION="python3.11"

# Create the required directory structure for Lambda layer
mkdir -p "$LAYER_DIR/python/lib/$PYTHON_VERSION/site-packages"

# Install dependencies into the layer
pip install -r "$LAYER_DIR/requirements.txt" \
    --platform manylinux2014_x86_64 \
    --python-version 311 \
    --implementation cp \
    --upgrade \
    -t "$LAYER_DIR/python/lib/$PYTHON_VERSION/site-packages" || \
pip install -r "$LAYER_DIR/requirements.txt" \
    -t "$LAYER_DIR/python/lib/$PYTHON_VERSION/site-packages"

echo "Lambda layer built successfully at $LAYER_DIR"
