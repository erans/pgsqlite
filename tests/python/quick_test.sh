#!/bin/bash

# Quick test to verify Poetry setup works
set -euo pipefail

echo "🧪 Quick Poetry Setup Test"
echo "=========================="
echo ""

# Check Poetry is available
if ! command -v poetry &> /dev/null; then
    echo "❌ Poetry not found"
    exit 1
fi

echo "✅ Poetry found: $(poetry --version)"

# Configure Poetry for local .venv
poetry config virtualenvs.in-project true --local
echo "✅ Poetry configured for local .venv"

# Show current config
echo ""
echo "📋 Poetry Configuration:"
poetry config --list | grep -E "(virtualenvs|cache)" || true
echo ""

# Try to install dependencies with shorter timeout
echo "📦 Installing dependencies (with timeout)..."
if timeout 120s poetry install --only main; then
    echo "✅ Dependencies installed successfully"
    
    # Test the environment
    echo ""
    echo "🧪 Testing environment..."
    if poetry run python test_poetry_setup.py; then
        echo ""
        echo "🎉 SUCCESS: Poetry environment is working!"
        exit 0
    else
        echo ""
        echo "❌ Environment test failed"
        exit 1
    fi
else
    echo ""
    echo "⚠️  Installation timed out or failed"
    echo "📄 You can manually run: poetry install --only main"
    echo "📄 Then test with: poetry run python test_poetry_setup.py"
    exit 1
fi