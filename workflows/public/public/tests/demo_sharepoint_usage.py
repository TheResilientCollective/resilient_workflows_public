#!/usr/bin/env python3
"""
Demo script for using SharepointResource with OIE SharePoint data.

This script demonstrates how to:
1. Connect to the OIE SharePoint site
2. Download the latest file from the shared documents
3. Upload it to S3/MinIO using the putStream method

Usage:
    python demo_sharepoint_usage.py

Environment Variables Required:
    SHAREPOINT_PASSWORD - Password for dwvalentine@ucsd.edu
    S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_ADDRESS - MinIO/S3 credentials
"""

import os
import sys
from datetime import datetime

# Add the parent directory to the path to import resources
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from resources.sharepoint import SharepointResource
from resources.minio import S3Resource


def main():
    """Main demo function."""

    # SharePoint configuration for OIE data
    sharepoint_config = {
        "SHAREPOINT_URL": "https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS/",
        "SHAREPOINT_USER": "dwvalentine@ucsd.edu",
        "SHAREPOINT_PASSWORD": os.getenv("SHAREPOINT_PASSWORD")
    }

    # S3/MinIO configuration
    s3_config = {
        "S3_BUCKET": os.getenv("S3_BUCKET", "test-bucket"),
        "S3_ADDRESS": os.getenv("S3_ADDRESS", "localhost"),
        "S3_PORT": os.getenv("S3_PORT", "9000"),
        "S3_USE_SSL": os.getenv("S3_USE_SSL", "false").lower() == "true",
        "S3_ACCESS_KEY": os.getenv("S3_ACCESS_KEY"),
        "S3_SECRET_KEY": os.getenv("S3_SECRET_KEY")
    }

    # Check required environment variables
    if not sharepoint_config["SHAREPOINT_PASSWORD"]:
        print("❌ Error: SHAREPOINT_PASSWORD environment variable is required")
        print("   Set it with: export SHAREPOINT_PASSWORD='your_password'")
        return 1

    if not all([s3_config["S3_ACCESS_KEY"], s3_config["S3_SECRET_KEY"]]):
        print("⚠️  Warning: S3 credentials not found. Will only demonstrate SharePoint download.")
        print("   To test S3 upload, set: S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_ADDRESS")
        s3_available = False
    else:
        s3_available = True

    try:
        # Initialize SharePoint resource
        print("🔗 Connecting to OIE SharePoint...")
        sharepoint_resource = SharepointResource(**sharepoint_config)

        # Test authentication
        ctx = sharepoint_resource.authenticate()
        print("✅ Successfully authenticated to SharePoint")

        # Download latest file from Shared Documents
        print("\n📁 Looking for latest file in Shared Documents...")
        folder_url = "/sites/PeriodicaldataextractionsOIE-WAHIS/Shared Documents"

        try:
            stream, filename = sharepoint_resource.latest_sharepoint_dataset(folder_url)
            file_size = stream.getbuffer().nbytes

            print(f"✅ Successfully downloaded: {filename}")
            print(f"   File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")

            # Reset stream position for potential upload
            stream.seek(0)

            # If S3 is available, demonstrate upload
            if s3_available:
                try:
                    print(f"\n☁️  Uploading to S3/MinIO...")
                    s3_resource = S3Resource(**s3_config)

                    # Create timestamped path
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    s3_path = f"sharepoint_downloads/{timestamp}_{filename}"

                    # Upload using putStream
                    result = s3_resource.putStream(
                        stream=stream,
                        length=file_size,  # We know the length for better performance
                        path=s3_path,
                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        metadata={"source": "sharepoint_oie", "original_filename": filename}
                    )

                    print(f"✅ Successfully uploaded to S3: {result}")
                    print(f"   S3 path: {s3_path}")

                except Exception as e:
                    print(f"❌ S3 upload failed: {e}")

            # Demonstrate file analysis
            print(f"\n📊 File Analysis:")
            print(f"   Filename: {filename}")
            print(f"   Extension: {filename.split('.')[-1] if '.' in filename else 'none'}")
            print(f"   Size: {file_size:,} bytes")

            # If it's an Excel file, we could analyze it further
            if filename.lower().endswith(('.xlsx', '.xls')):
                print("   Type: Excel spreadsheet")
                print("   💡 Tip: You could use pandas.read_excel(stream) to analyze the data")

        except FileNotFoundError as e:
            print(f"❌ No files found: {e}")
            return 1

        except Exception as e:
            print(f"❌ Download failed: {e}")
            return 1

    except Exception as e:
        print(f"❌ SharePoint connection failed: {e}")
        return 1

    print("\n🎉 Demo completed successfully!")
    return 0


def test_sharepoint_folders():
    """Test different SharePoint folder paths."""

    sharepoint_config = {
        "SHAREPOINT_URL": "https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS/",
        "SHAREPOINT_USER": "dwvalentine@ucsd.edu",
        "SHAREPOINT_PASSWORD": os.getenv("SHAREPOINT_PASSWORD")
    }

    if not sharepoint_config["SHAREPOINT_PASSWORD"]:
        print("❌ SHAREPOINT_PASSWORD required for folder testing")
        return

    sharepoint_resource = SharepointResource(**sharepoint_config)

    # Test different folder paths
    test_folders = [
        "/sites/PeriodicaldataextractionsOIE-WAHIS/Shared Documents",
        "/sites/PeriodicaldataextractionsOIE-WAHIS/Shared Documents/General",
        "/sites/PeriodicaldataextractionsOIE-WAHIS/Documents",
    ]

    print("🔍 Testing different SharePoint folder paths...")

    for folder_path in test_folders:
        try:
            print(f"\n📁 Testing: {folder_path}")
            stream, filename = sharepoint_resource.latest_sharepoint_dataset(folder_path)
            print(f"   ✅ Found latest file: {filename}")
        except Exception as e:
            print(f"   ❌ Failed: {e}")


def analyze_sharepoint_link():
    """Analyze the provided SharePoint file link."""

    file_link = "https://oieoffice365.sharepoint.com/:x:/r/sites/PeriodicaldataextractionsOIE-WAHIS/Shared%20Documents/infur_20251103.xlsx?d=w0da57f5dc22046d094c1aa3fb59ed39d&csf=1&web=1&e=BoYQVN"

    print("🔗 Analyzing SharePoint file link:")
    print(f"   Link: {file_link}")

    # Parse the link components
    if ":x:" in file_link:
        print("   Type: Excel file sharing link")

    # Extract filename from URL
    if "infur_20251103.xlsx" in file_link:
        print("   Expected filename: infur_20251103.xlsx")
        print("   Date: 2025-11-03 (from filename)")

    # Extract site path
    site_part = file_link.split("/sites/")[1].split("/")[0]
    print(f"   Site: {site_part}")

    # Note about URL encoding
    print("   📝 Note: SharePoint sharing links need to be converted to server-relative URLs")
    print("           The API uses paths like '/sites/sitename/Shared Documents/filename.xlsx'")


if __name__ == "__main__":
    print("🧪 SharePoint Resource Demo")
    print("=" * 50)

    # Show link analysis
    analyze_sharepoint_link()
    print()

    # Run main demo
    exit_code = main()

    # Optionally test folder paths
    if exit_code == 0 and input("\n🔍 Test different folder paths? (y/n): ").lower() == 'y':
        test_sharepoint_folders()

    sys.exit(exit_code)