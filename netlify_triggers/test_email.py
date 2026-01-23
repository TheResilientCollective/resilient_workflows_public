#!/usr/bin/env python3
"""
Test script for email functionality in the Netlify Triggers system.

This script tests the email sending functionality without requiring
the full Slack/Netlify integration.

Usage:
    python test_email.py

Make sure to set the required environment variables before running:
    export EMAIL_ENABLED="true"
    export EMAIL_FROM="your-email@example.com"
    export EMAIL_PASSWORD="your-password"
    export EMAIL_TO="recipient@example.com"
"""

import os
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def load_config():
    """Load email configuration from environment variables."""
    config = {
        'EMAIL_ENABLED': os.environ.get("EMAIL_ENABLED", "false").lower() == "true",
        'EMAIL_SMTP_SERVER': os.environ.get("EMAIL_SMTP_SERVER", "smtp.gmail.com"),
        'EMAIL_SMTP_PORT': int(os.environ.get("EMAIL_SMTP_PORT", 587)),
        'EMAIL_FROM': os.environ.get("EMAIL_FROM"),
        'EMAIL_PASSWORD': os.environ.get("EMAIL_PASSWORD"),
        'EMAIL_TO': os.environ.get("EMAIL_TO")
    }
    return config

def send_test_email(config):
    """Send a test email using the provided configuration."""
    if not config['EMAIL_ENABLED']:
        print("❌ Email not enabled. Set EMAIL_ENABLED='true'")
        return False

    if not all([config['EMAIL_FROM'], config['EMAIL_PASSWORD'], config['EMAIL_TO']]):
        print("❌ Missing required email configuration:")
        print(f"   EMAIL_FROM: {'✓' if config['EMAIL_FROM'] else '❌'}")
        print(f"   EMAIL_PASSWORD: {'✓' if config['EMAIL_PASSWORD'] else '❌'}")
        print(f"   EMAIL_TO: {'✓' if config['EMAIL_TO'] else '❌'}")
        return False

    try:
        # Create test message
        subject = "Test Email from Netlify Triggers System"
        body = """This is a test email from the Netlify Triggers system.

If you received this email, the email configuration is working correctly.

Test Details:
- SMTP Server: {smtp_server}
- SMTP Port: {smtp_port}
- From: {email_from}
- To: {email_to}

This email was sent using the test_email.py script.""".format(
            smtp_server=config['EMAIL_SMTP_SERVER'],
            smtp_port=config['EMAIL_SMTP_PORT'],
            email_from=config['EMAIL_FROM'],
            email_to=config['EMAIL_TO']
        )

        html_body = f"""
<!DOCTYPE html>
<html>
<body>
    <h2>✅ Test Email from Netlify Triggers System</h2>
    <p>If you received this email, the email configuration is working correctly.</p>

    <h3>Test Details:</h3>
    <ul>
        <li><strong>SMTP Server:</strong> {config['EMAIL_SMTP_SERVER']}</li>
        <li><strong>SMTP Port:</strong> {config['EMAIL_SMTP_PORT']}</li>
        <li><strong>From:</strong> {config['EMAIL_FROM']}</li>
        <li><strong>To:</strong> {config['EMAIL_TO']}</li>
    </ul>

    <p>This email was sent using the <code>test_email.py</code> script.</p>

    <hr>
    <small>Netlify Triggers System - Email Test</small>
</body>
</html>"""

        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = config['EMAIL_FROM']
        msg['Subject'] = subject

        # Handle multiple recipients
        recipients = [email.strip() for email in config['EMAIL_TO'].split(',')]
        msg['To'] = ', '.join(recipients)

        # Add text and HTML parts
        msg.attach(MIMEText(body, 'plain'))
        msg.attach(MIMEText(html_body, 'html'))

        # Send email
        print(f"🔄 Connecting to SMTP server {config['EMAIL_SMTP_SERVER']}:{config['EMAIL_SMTP_PORT']}...")

        with smtplib.SMTP(config['EMAIL_SMTP_SERVER'], config['EMAIL_SMTP_PORT']) as server:
            print("🔄 Starting TLS...")
            server.starttls()

            print("🔄 Authenticating...")
            server.login(config['EMAIL_FROM'], config['EMAIL_PASSWORD'])

            print(f"🔄 Sending email to {', '.join(recipients)}...")
            server.send_message(msg, to_addrs=recipients)

        print(f"✅ Test email sent successfully to {', '.join(recipients)}")
        return True

    except Exception as e:
        print(f"❌ Error sending test email: {str(e)}")
        return False

def main():
    """Main function to run the email test."""
    print("🧪 Testing Netlify Triggers Email Configuration")
    print("=" * 50)

    # Load configuration
    config = load_config()

    # Display configuration
    print("📧 Email Configuration:")
    print(f"   Enabled: {config['EMAIL_ENABLED']}")
    print(f"   SMTP Server: {config['EMAIL_SMTP_SERVER']}")
    print(f"   SMTP Port: {config['EMAIL_SMTP_PORT']}")
    print(f"   From: {config['EMAIL_FROM'] or 'NOT SET'}")
    print(f"   Password: {'SET' if config['EMAIL_PASSWORD'] else 'NOT SET'}")
    print(f"   To: {config['EMAIL_TO'] or 'NOT SET'}")
    print()

    # Send test email
    success = send_test_email(config)

    if success:
        print("\n🎉 Email test completed successfully!")
        print("Check the recipient inbox for the test email.")
    else:
        print("\n💥 Email test failed!")
        print("Please check your configuration and try again.")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())