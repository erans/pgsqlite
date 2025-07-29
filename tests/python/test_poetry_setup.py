#!/usr/bin/env python3
"""
Test script to verify Poetry environment setup works correctly.
This is a quick test to verify that the dependencies are properly installed.
"""

import sys
import argparse

def test_imports_and_versions():
    """Test that all required packages can be imported and show versions."""
    print("🧪 Testing Poetry Environment Setup")
    print("==================================")
    print("")
    
    success = True
    
    # Test SQLAlchemy
    try:
        import sqlalchemy
        print(f"✅ SQLAlchemy version: {sqlalchemy.__version__}")
    except ImportError as e:
        print(f"❌ SQLAlchemy import failed: {e}")
        success = False
    
    # Test psycopg2
    try:
        import psycopg2
        print(f"✅ psycopg2 version: {psycopg2.__version__}")
    except ImportError as e:
        print(f"❌ psycopg2 import failed: {e}")
        success = False
    
    # Test Python environment
    print(f"✅ Python version: {sys.version.split()[0]}")
    print(f"✅ Python executable: {sys.executable}")
    
    # Check if we're in a virtual environment
    import os
    venv = os.environ.get('VIRTUAL_ENV')
    if venv:
        print(f"✅ Virtual environment: {venv}")
    else:
        print("⚠️  No virtual environment detected")
    
    return success

def test_basic_sqlalchemy():
    """Test basic SQLAlchemy functionality."""
    try:
        from sqlalchemy import create_engine, text
        
        print("\n🔍 Testing basic SQLAlchemy functionality...")
        
        # Test engine creation (without connecting)
        engine = create_engine("sqlite:///:memory:", echo=False)
        print("✅ SQLAlchemy engine creation successful")
        
        # Test with in-memory SQLite
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test")).fetchone()
            print(f"✅ Basic SQL execution: {result[0]}")
        
        return True
    except Exception as e:
        print(f"❌ Basic SQLAlchemy test failed: {e}")
        return False

def main():
    """Main test function."""
    parser = argparse.ArgumentParser(description="Test Poetry environment setup")
    args = parser.parse_args()
    
    # Run tests
    imports_ok = test_imports_and_versions()
    sqlalchemy_ok = test_basic_sqlalchemy()
    
    print("\n" + "="*50)
    
    if imports_ok and sqlalchemy_ok:
        print("🎉 SUCCESS: Poetry environment is set up correctly!")
        print("✅ All dependencies are installed and working")
        return 0
    else:
        print("❌ FAILURE: Poetry environment setup has issues")
        print("Please run: poetry install --only main")
        return 1

if __name__ == "__main__":
    sys.exit(main())