#!/bin/bash
#
# Step 2: Download a File
#
# This script reads the token from 'access_token.txt'
# and uses it to download a file from SharePoint.
#

# --- CONFIGURATION ---
# Your SharePoint site's hostname
SHAREPOINT_HOST="oieoffice365.sharepoint.com"

# The path to your site.
# For the root site: "/"
# For a sub-site:  "/sites/MySiteName"
SITE_PATH="/sites/PeriodicaldataextractionsOIE-WAHIS"

# The path to your file *within* the site's default Document Library.
# IMPORTANT: This path is relative to the "root" of the drive,
# which is *usually* the "Shared Documents" library.
# Example: "/MyFolder/MyReport.xlsx"
# This corresponds to a file in:
# "Shared Documents" -> "MyFolder" -> "MyReport.xlsx"
FILE_PATH="/Shared Documents/infur_20251103.xlsx"
#FILE_PATH="/Forms/infur_20251103.xlsx"
#FILE_PATH="/infur_20251103.xlsx"
# Where to save the downloaded file
OUTPUT_FILE="infur_20251103.xlsx"
# --- END CONFIGURATION ---

# Check if the token file exists
if [ ! -f "access_token.txt" ]; then
    echo "access_token.txt not found. Please run 1_get_token.sh first."
    exit 1
fi

# Read the token from the file
ACCESS_TOKEN=$(cat access_token.txt)
# URL-encode the file path to handle spaces.
# This replaces spaces with %20, which is required for the cURL request.
FILE_PATH_ENCODED=$(echo "$FILE_PATH" | sed 's/ /%20/g')

# Construct the Microsoft Graph API URL
# This format targets a file by its path.
API_URL="https://graph.microsoft.com/v1.0/sites/$SHAREPOINT_HOST:$SITE_PATH/drive/root:$FILE_PATH_ENCODED:/content"
#API_URL="https://graph.microsoft.com/v1.0/sites/$SHAREPOINT_HOST:$SITE_PATH/drive$FILE_PATH_ENCODED:/content"

echo "Downloading file from $API_URL..."

 Make the GET request to download the file
# -L : Follows redirects
# -o : Saves the output to a file
# -s : Silent mode
# -w "%{http_code}" : Writes the HTTP status code to stdout
HTTP_STATUS=$(curl  -L -s -X GET "$API_URL" \
-H "Authorization: Bearer $ACCESS_TOKEN" \
-o "$OUTPUT_FILE" \
-w "%{http_code}")

# Check the HTTP status code
if [ "$HTTP_STATUS" -eq 200 ]; then
    echo "Success! (HTTP 200 OK). File downloaded to $OUTPUT_FILE"
else
    echo "Error: File download failed with HTTP status $HTTP_STATUS."
    echo "Please check paths and token permissions (see README)."
    echo ""
    echo "--- API Error Response (from $OUTPUT_FILE) ---"
    # The output file contains the error JSON, so we print it.
    cat "$OUTPUT_FILE"
    echo ""
    echo "------------------------------------------------"

    # Clean up the file, as it just contains an error
    rm "$OUTPUT_FILE"
    exit 1
fi
