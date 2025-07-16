#!/bin/bash
set -euo pipefail

LAYER_NAME="data-services-layer"
TMP_BUILD_DIR=".layer_build_tmp"
OUTPUT_ZIP="${LAYER_NAME}.zip"

# Clean up from previous builds
rm -rf "$TMP_BUILD_DIR" "$OUTPUT_ZIP"

# Create directory structure
mkdir -p "$TMP_BUILD_DIR/python/lib/python3.13/site-packages"

# Install dependencies into the temp build dir
pip install -r requirements.txt --target "$TMP_BUILD_DIR/python/lib/python3.13/site-packages"

# Zip the contents
cd "$TMP_BUILD_DIR"
zip -r "../$OUTPUT_ZIP" .
cd ..

# Clean up
rm -rf "$TMP_BUILD_DIR"

echo "Created $OUTPUT_ZIP cleanly."
