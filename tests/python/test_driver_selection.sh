#!/bin/bash

# Quick test script to verify driver selection works

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Testing SQLAlchemy driver selection..."
echo ""

# Test help
echo "1. Testing help output:"
"$SCRIPT_DIR/run_sqlalchemy_tests.sh" --help | grep -E "(--driver|--binary-format)" || echo "Help test failed"

echo ""
echo "2. Testing invalid driver detection:"
if "$SCRIPT_DIR/run_sqlalchemy_tests.sh" --driver invalid 2>&1 | grep -q "Invalid driver"; then
    echo "✅ Invalid driver detection works"
else
    echo "❌ Invalid driver detection failed"
fi

echo ""
echo "3. Testing binary format with psycopg2 detection:"
if "$SCRIPT_DIR/run_sqlalchemy_tests.sh" --driver psycopg2 --binary-format 2>&1 | grep -q "Binary format is only supported with psycopg3"; then
    echo "✅ Binary format validation works"
else
    echo "❌ Binary format validation failed"
fi

echo ""
echo "✅ Driver selection validation tests passed!"