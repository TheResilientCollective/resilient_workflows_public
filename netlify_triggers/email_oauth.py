#!/usr/bin/env python3
"""
OAuth2-based email functionality for Office 365/Microsoft Graph API.

This module provides OAuth2 authentication for sending emails through
Microsoft Graph API instead of SMTP, which is required for many
Office 365 organizations that have disabled basic authentication.
"""

import os
import logging
import requests
import json
from typing import Optional, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class Office365OAuth:
    """Office 365 OAuth2 email sender using Microsoft Graph API"""

    def __init__(self):
        """Initialize with OAuth2 configuration from environment variables"""
        self.client_id = os.environ.get("OFFICE365_CLIENT_ID")
        self.client_secret = os.environ.get("OFFICE365_CLIENT_SECRET")
        self.tenant_id = os.environ.get("OFFICE365_TENANT_ID")
        self.scope = "https://graph.microsoft.com/.default"
        self.graph_url = "https://graph.microsoft.com/v1.0"
        self.access_token = None
        self.token_expires_at = None

    def is_configured(self) -> bool:
        """Check if OAuth2 is properly configured"""
        return all([self.client_id, self.client_secret, self.tenant_id])

    def get_access_token(self) -> Optional[str]:
        """Get OAuth2 access token using client credentials flow"""
        try:
            # Check if we have a valid token
            if (self.access_token and self.token_expires_at and
                datetime.now() < self.token_expires_at):
                return self.access_token

            # Get new token
            token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": self.scope
            }

            response = requests.post(token_url, data=data)
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data["access_token"]

            # Set expiration time (subtract 5 minutes for safety)
            expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 300)

            logger.info("Successfully obtained OAuth2 access token")
            return self.access_token

        except Exception as e:
            logger.error(f"Error getting OAuth2 access token: {str(e)}")
            return None

    def send_email(self, from_email: str, to_emails: List[str], subject: str,
                   body: str, html_body: Optional[str] = None) -> bool:
        """Send email using Microsoft Graph API"""
        try:
            access_token = self.get_access_token()
            if not access_token:
                logger.error("No valid access token available")
                return False

            # Prepare recipients
            recipients = [{"emailAddress": {"address": email}} for email in to_emails]

            # Prepare message body
            content = {
                "contentType": "HTML" if html_body else "Text",
                "content": html_body if html_body else body
            }

            # Prepare the email message
            message = {
                "subject": subject,
                "body": content,
                "toRecipients": recipients
            }

            # Send the email
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }

            # Extract username from email for API call
            username = from_email.split('@')[0] if '@' in from_email else from_email
            send_url = f"{self.graph_url}/users/{from_email}/sendMail"

            payload = {"message": message}

            response = requests.post(send_url, headers=headers, json=payload)
            response.raise_for_status()

            logger.info(f"Email sent successfully to {', '.join(to_emails)} via Microsoft Graph API")
            return True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.error("Permission denied. App may not have Mail.Send permission")
            elif e.response.status_code == 404:
                logger.error(f"User {from_email} not found or no mailbox")
            else:
                logger.error(f"HTTP error sending email: {e.response.status_code} - {e.response.text}")
            return False

        except Exception as e:
            logger.error(f"Error sending email via Microsoft Graph: {str(e)}")
            return False


def send_email_oauth(subject: str, body: str, html_body: Optional[str] = None) -> bool:
    """
    Send email using OAuth2 authentication.

    This function uses the same environment variables as the SMTP version
    but adds OAuth2 support for Office 365.
    """
    # Check if OAuth2 is configured
    oauth = Office365OAuth()
    if not oauth.is_configured():
        logger.info("OAuth2 not configured, falling back to SMTP")
        return False

    # Get email configuration
    email_from = os.environ.get("EMAIL_FROM")
    email_to = os.environ.get("EMAIL_TO")
    email_enabled = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"

    if not email_enabled or not email_from or not email_to:
        logger.info("Email not configured or disabled, skipping email notification")
        return False

    # Handle multiple recipients
    recipients = [email.strip() for email in email_to.split(',')]

    # Send email using OAuth2
    return oauth.send_email(email_from, recipients, subject, body, html_body)


if __name__ == "__main__":
    """Test OAuth2 email functionality"""
    import sys

    print("🧪 Testing OAuth2 Email Configuration")
    print("=" * 50)

    oauth = Office365OAuth()

    # Check configuration
    print("📧 OAuth2 Configuration:")
    print(f"   Client ID: {'SET' if oauth.client_id else 'NOT SET'}")
    print(f"   Client Secret: {'SET' if oauth.client_secret else 'NOT SET'}")
    print(f"   Tenant ID: {'SET' if oauth.tenant_id else 'NOT SET'}")
    print(f"   Configured: {oauth.is_configured()}")
    print()

    if not oauth.is_configured():
        print("❌ OAuth2 not configured. Please set:")
        print("   OFFICE365_CLIENT_ID")
        print("   OFFICE365_CLIENT_SECRET")
        print("   OFFICE365_TENANT_ID")
        sys.exit(1)

    # Test token acquisition
    print("🔄 Testing OAuth2 token acquisition...")
    token = oauth.get_access_token()

    if token:
        print("✅ OAuth2 token acquired successfully")

        # Test email sending
        email_from = os.environ.get("EMAIL_FROM")
        email_to = os.environ.get("EMAIL_TO")

        if email_from and email_to:
            print(f"🔄 Sending test email from {email_from} to {email_to}...")

            success = send_email_oauth(
                "Test Email - OAuth2",
                "This is a test email sent using OAuth2 authentication.",
                "<p>This is a <strong>test email</strong> sent using OAuth2 authentication.</p>"
            )

            if success:
                print("🎉 OAuth2 email test completed successfully!")
            else:
                print("💥 OAuth2 email test failed!")
                sys.exit(1)
        else:
            print("⚠️  EMAIL_FROM or EMAIL_TO not set, skipping email test")
    else:
        print("❌ Failed to acquire OAuth2 token")
        sys.exit(1)