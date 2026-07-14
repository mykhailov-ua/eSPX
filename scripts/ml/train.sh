#!/bin/bash
set -e

# Ensure we are in the workspace root
cd "$(dirname "$0")/../.."

echo "Starting ML training pipeline..."
python3 scripts/ml/train.py
