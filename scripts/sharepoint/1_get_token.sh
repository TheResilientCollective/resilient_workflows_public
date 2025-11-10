#!/bin/bash
#
# Step 1: Get an Access Token
#
# This script exchanges your app credentials for an access token
# and saves it to a file named 'access_token.txt'.
#

# --- CONFIGURATION ---
# Fill in these values from your Azure App Registration
TENANT_ID="8a198873-4fec-4e76-8182-ca479edbbd60"
CLIENT_ID="d5dd99ba-7269-4174-a61c-69b6e31e672c"
# Path to a file containing your client secret.
# This is more secure than hardcoding it.
# See README.md for instructions.
SECRET_FILE_PATH="client.secret"

# This is the standard scope for Microsoft Graph API.
# It requests permission for all *application* permissions
# you have already granted in the Azure portal.
#SCOPE="https://oieoffice365.sharepoint.com/.default"
SCOPE="https://graph.microsoft.com/.default"

# --- END CONFIGURATION ---

# --- Read Secret from File ---
# Check if the secret file exists
if [ ! -f "$SECRET_FILE_PATH" ]; then
    echo "Error: Client secret file not found at '$SECRET_FILE_PATH'"
    echo "Please create this file and place *only* your client secret inside it."
    echo "Run: echo \"your_secret_here\" > $SECRET_FILE_PATH"
    exit 1
fi

# Read the secret from the file.
# This avoids having the secret in your script or shell history.
CLIENT_SECRET=$(cat "$SECRET_FILE_PATH")

# Check if the secret file was empty
if [ -z "$CLIENT_SECRET" ]; then
    echo "Error: The secret file at '$SECRET_FILE_PATH' is empty."
    echo "Please add your client secret to this file."
    exit 1
fi
# --- End Read Secret ---

# --- END CONFIGURATION ---

# Define the token endpoint
TOKEN_ENDPOINT="https://login.microsoftonline.com/$TENANT_ID/oauth2/v2.0/token"

# Create a temporary file to hold the full JSON response
TOKEN_RESPONSE_FILE="token_response.json"

echo "Requesting access token..."

# Make the POST request to get the token
curl -X POST -s "$TOKEN_ENDPOINT" \
-H "Content-Type: application/x-www-form-urlencoded" \
-d "client_id=$CLIENT_ID" \
-d "client_secret=$CLIENT_SECRET" \
-d "scope=$SCOPE" \
-d "grant_type=client_credentials" \
-o $TOKEN_RESPONSE_FILE

# Parse the access_token from the JSON response.
# This uses grep and cut, which works on most systems without 'jq'.
ACCESS_TOKEN=$(cat $TOKEN_RESPONSE_FILE | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)

# Check if we actually got a token
if [ -z "$ACCESS_TOKEN" ]; then
    echo "Error: Could not get access token. Check credentials and permissions."
    echo "Full server response:"
    cat $TOKEN_RESPONSE_FILE
    rm $TOKEN_RESPONSE_FILE
    exit 1
fi

# Save the token to a file for the next script to use
echo $ACCESS_TOKEN > access_token.txt

# Clean up the full response
rm $TOKEN_RESPONSE_FILE

echo "Success! Access token saved to access_token.txt"

