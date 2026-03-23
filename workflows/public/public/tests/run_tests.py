#!/usr/bin/env python3
"""
Test runner for SharePoint resource tests.

This script provides an easy way to run different types of tests:
- Unit tests (mocked, fast)
- Integration tests (require real credentials)
- Demo script

Usage:
    python run_tests.py [test_type]

Test types:
    unit        - Run unit tests only (default)
    integration - Run integration tests (requires SHAREPOINT_PASSWORD)
    all         - Run all tests
    demo        - Run the demo script
    help        - Show this help
"""

import sys
import os
import subprocess
from pathlib import Path


def run_command(cmd, description):
    """Run a command and return the result."""
    print(f"\n🏃 {description}")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 50)

    try:
        result = subprocess.run(cmd, check=True, cwd=Path(__file__).parent)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"❌ Command not found: {cmd[0]}")
        print("Make sure pytest is installed: pip install pytest")
        return False


def check_requirements():
    """Check if required packages are installed."""
    print("🔍 Checking requirements...")

    required_packages = [
        ("pytest", "pip install pytest"),
        ("office365", "pip install Office365-REST-Python-Client"),
        ("minio", "pip install minio"),
    ]

    missing_packages = []

    for package, install_cmd in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} is installed")
        except ImportError:
            print(f"❌ {package} is missing")
            missing_packages.append((package, install_cmd))

    if missing_packages:
        print("\n📦 Missing packages found. Install them with:")
        for package, install_cmd in missing_packages:
            print(f"   {install_cmd}")
        return False

    return True


def run_unit_tests():
    """Run unit tests only."""
    cmd = [
        "python", "-m", "pytest",
        "test_sharepoint_resource.py",
        "-m", "not integration",
        "-v"
    ]
    return run_command(cmd, "Running unit tests")


def run_integration_tests():
    """Run integration tests."""
    if not os.getenv("SHAREPOINT_PASSWORD"):
        print("⚠️  SHAREPOINT_PASSWORD environment variable not set")
        print("   Integration tests will be skipped")
        print("   Set it with: export SHAREPOINT_PASSWORD='your_password'")

    cmd = [
        "python", "-m", "pytest",
        "test_sharepoint_resource.py",
        "-m", "integration",
        "-v", "-s"
    ]
    return run_command(cmd, "Running integration tests")


def run_all_tests():
    """Run all tests."""
    cmd = [
        "python", "-m", "pytest",
        "test_sharepoint_resource.py",
        "-v"
    ]
    return run_command(cmd, "Running all tests")


def run_demo():
    """Run the demo script."""
    cmd = ["python", "demo_sharepoint_usage.py"]
    return run_command(cmd, "Running SharePoint demo")


def show_help():
    """Show help information."""
    print(__doc__)

    print("\n📋 Environment Variables:")
    print("   SHAREPOINT_PASSWORD - Required for integration tests and demo")
    print("   S3_ACCESS_KEY       - Required for S3 upload demo")
    print("   S3_SECRET_KEY       - Required for S3 upload demo")
    print("   S3_BUCKET           - S3 bucket name")
    print("   S3_ADDRESS          - S3 endpoint address")

    print("\n🔧 Setup:")
    print("   1. Install requirements: pip install pytest Office365-REST-Python-Client minio")
    print("   2. Set environment variables")
    print("   3. Run tests: python run_tests.py unit")

    print("\n📁 Files:")
    print("   test_sharepoint_resource.py - Main test file")
    print("   demo_sharepoint_usage.py    - Demo script")
    print("   pytest.ini                  - Pytest configuration")
    print("   run_tests.py                - This test runner")


def main():
    """Main function."""
    test_type = sys.argv[1] if len(sys.argv) > 1 else "unit"

    print("🧪 SharePoint Resource Test Runner")
    print("=" * 50)

    if test_type == "help":
        show_help()
        return 0

    # Check requirements first
    if not check_requirements():
        print("\n❌ Requirements check failed. Please install missing packages.")
        return 1

    # Run the specified test type
    success = True

    if test_type == "unit":
        success = run_unit_tests()
    elif test_type == "integration":
        success = run_integration_tests()
    elif test_type == "all":
        success = run_unit_tests() and run_integration_tests()
    elif test_type == "demo":
        success = run_demo()
    else:
        print(f"❌ Unknown test type: {test_type}")
        print("Valid options: unit, integration, all, demo, help")
        return 1

    # Summary
    print("\n" + "=" * 50)
    if success:
        print("🎉 All tests completed successfully!")
        return 0
    else:
        print("❌ Some tests failed. Check the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())