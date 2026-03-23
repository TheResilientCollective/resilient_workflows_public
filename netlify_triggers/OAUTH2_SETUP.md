# OAuth2 Setup for Office 365 Email

This document explains how to set up OAuth2 authentication for sending emails through Microsoft Graph API, which is required for many Office 365/UCSD accounts that have disabled basic SMTP authentication.

## Why OAuth2?

Your current error `Authentication unsuccessful, the request did not meet the criteria to be authenticated successfully` indicates that your UCSD Office 365 account requires modern authentication (OAuth2) instead of basic SMTP authentication with username/password.

## Setup Steps

### Step 1: Create Azure AD App Registration

1. **Sign in to Azure Portal**:
   - Go to https://portal.azure.com
   - Sign in with your UCSD credentials or the admin account for your organization

2. **Navigate to App Registrations**:
   - Search for "App registrations" in the top search bar
   - Click on "App registrations" under Services

3. **Create New Registration**:
   - Click "New registration"
   - Fill out the form:
     - **Name**: `Netlify Triggers Email Service` (or any descriptive name)
     - **Supported account types**: "Accounts in this organizational directory only (UCSD only)"
     - **Redirect URI**: Leave blank for now
   - Click "Register"

4. **Note Your Application Details**:
   After registration, you'll see the app overview page. Copy these values:
   - **Application (client) ID** → This becomes `OFFICE365_CLIENT_ID`
   - **Directory (tenant) ID** → This becomes `OFFICE365_TENANT_ID`

### Step 2: Create Client Secret

1. **Navigate to Certificates & secrets**:
   - In your app registration, click "Certificates & secrets" in the left menu

2. **Create New Client Secret**:
   - Click "New client secret"
   - Add a description: "Netlify Triggers Email"
   - Set expiration (recommend "24 months")
   - Click "Add"

3. **Copy the Secret Value**:
   - **Important**: Copy the secret VALUE immediately (not the ID)
   - This becomes `OFFICE365_CLIENT_SECRET`
   - You can't see this value again after leaving the page!

### Step 3: Configure API Permissions

1. **Navigate to API permissions**:
   - In your app registration, click "API permissions"

2. **Add Required Permissions**:
   - Click "Add a permission"
   - Choose "Microsoft Graph"
   - Choose "Application permissions"
   - Search for and add these permissions:
     - `Mail.Send` - Send mail as any user
     - `User.Read.All` - Read all users' profiles (needed to send on behalf of users)

3. **Grant Admin Consent**:
   - After adding permissions, click "Grant admin consent for [Your Organization]"
   - This step requires admin privileges in your organization
   - If you don't have admin access, you'll need to request approval from your IT department

### Step 4: Update Environment Variables

Update your `netlify_triggers/.env` file with the values from Azure:

```bash
# OAuth2 Configuration for Office 365
OFFICE365_CLIENT_ID=your-application-client-id-here
OFFICE365_CLIENT_SECRET=your-client-secret-value-here
OFFICE365_TENANT_ID=your-tenant-id-here
```

### Step 5: Test OAuth2 Email

```bash
cd netlify_triggers
source .env
python email_oauth.py
```

If successful, you should see:
```
✅ OAuth2 token acquired successfully
🎉 OAuth2 email test completed successfully!
```

## For UCSD Specific Setup

### Tenant ID for UCSD
If you're using UCSD email, your tenant ID is likely:
- **UCSD Tenant ID**: You can find this by going to https://portal.azure.com and checking the tenant ID in the top-right corner

### Getting Admin Consent
If you don't have admin privileges to grant consent:

1. **Contact UCSD IT**: Explain that you need an Azure AD app registration with `Mail.Send` and `User.Read.All` permissions for automated email notifications

2. **Alternative - Delegated Permissions**: If application permissions aren't approved, you can try delegated permissions with user authentication flow (more complex)

## Troubleshooting

### Common Issues

1. **"Insufficient privileges"**:
   - Make sure admin consent was granted
   - Verify the correct permissions are added

2. **"Application not found"**:
   - Double-check the Client ID and Tenant ID
   - Ensure the app registration exists in the correct tenant

3. **"Authentication failed"**:
   - Verify the client secret is correct
   - Check if the secret has expired

4. **"Permission denied"**:
   - The application may not have Mail.Send permission
   - Admin consent may not have been granted

### Testing Individual Components

Test OAuth2 token acquisition only:
```bash
cd netlify_triggers
python -c "
import os
from email_oauth import Office365OAuth
oauth = Office365OAuth()
token = oauth.get_access_token()
print('Token acquired!' if token else 'Token failed!')
"
```

## Fallback Options

If OAuth2 setup is not possible:

1. **App Passwords** (if available):
   - Some organizations allow app-specific passwords
   - Check your Microsoft account security settings

2. **Different Email Provider**:
   - Use a personal Gmail account with app passwords
   - Update `EMAIL_FROM` and use Gmail SMTP settings

3. **SMTP Relay Service**:
   - Use services like SendGrid, Mailgun, or AWS SES
   - These provide SMTP credentials that work with basic auth

## Security Notes

- **Never commit secrets**: Keep your `.env` file out of version control
- **Rotate secrets regularly**: Set calendar reminders to update client secrets
- **Principle of least privilege**: Only request the minimum permissions needed
- **Monitor usage**: Check Azure AD sign-in logs for unusual activity

## Integration with Current System

The updated `app.py` will:
1. Try OAuth2 first (if configured)
2. Fall back to SMTP if OAuth2 fails or isn't configured
3. Log which method was used for debugging

This means you can gradually migrate from SMTP to OAuth2 without breaking existing functionality.