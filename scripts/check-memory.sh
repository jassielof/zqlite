#!/bin/bash
# ZQLite Memory Leak Detection - Zig 0.16.0-dev
# Runs tests with memory leak detection enabled

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "=== ZQLite Memory Leak Check ==="
echo "Zig version: $(zig version)"
echo ""

# Run tests - Zig's GeneralPurposeAllocator detects leaks automatically
echo "Running tests with GPA leak detection..."
if zig build test -Dprofile=full --summary all 2>&1; then
    echo ""
    echo "✓ No memory leaks detected"
else
    echo ""
    echo "✗ Tests failed or memory leaks detected"
    exit 1
fi
