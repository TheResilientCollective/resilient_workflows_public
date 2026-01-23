# Email Notification Setup

This document explains how to configure email notifications for the Netlify Triggers system.

## Overview

The `handle_approve_deploy` function now sends email notifications when deployments are approved. This feature is optional and can be enabled through environment variables.

## Environment Variables

To enable email notifications, configure these environment variables:

### Required Variables
- `EMAIL_ENABLED`: Set to `"true"` to enable email notifications (default: `"false"`)
- `EMAIL_FROM`: The sender email address (e.g., `"notifications@example.com"`)
- `EMAIL_PASSWORD`: The password or app-specific password for the sender email
- `EMAIL_TO`: Recipient email addresses (comma-separated for multiple recipients: `"user1@example.com,user2@example.com"`)

### Optional Variables
- `EMAIL_SMTP_SERVER`: SMTP server hostname (default: `"smtp.gmail.com"`)
- `EMAIL_SMTP_PORT`: SMTP server port (default: `587`)

## Example Configuration

```bash
# Enable email notifications
export EMAIL_ENABLED="true"

# Gmail configuration (most common)
export EMAIL_FROM="your-notifications@gmail.com"
export EMAIL_PASSWORD="your-app-password"  # Use App Password, not regular password
export EMAIL_TO="admin@example.com,devops@example.com"

# Optional: Custom SMTP server
export EMAIL_SMTP_SERVER="smtp.custom-server.com"
export EMAIL_SMTP_PORT="587"
```

## Gmail Setup

If using Gmail, you'll need to:

1. Enable 2-factor authentication on your Google account
2. Generate an App Password:
   - Go to Google Account settings → Security → 2-Step Verification → App Passwords
   - Generate a new app password for "Mail"
   - Use this app password as `EMAIL_PASSWORD`

## Other Email Providers

For other email providers, update the SMTP settings accordingly:

### Outlook/Hotmail
```bash
export EMAIL_SMTP_SERVER="smtp.live.com"
export EMAIL_SMTP_PORT="587"
```

### Yahoo
```bash
export EMAIL_SMTP_SERVER="smtp.mail.yahoo.com"
export EMAIL_SMTP_PORT="587"
```

### Custom SMTP
```bash
export EMAIL_SMTP_SERVER="mail.your-domain.com"
export EMAIL_SMTP_PORT="587"  # or 465 for SSL
```

## Email Content

When a deployment is approved, recipients will receive:

- **Subject**: "Deployment Approved - Production Build Triggered"
- **Content**: Details about the approved deployment including:
  - User who approved the deployment
  - Preview URL
  - Production URL
  - Deployment hook URL

The email is sent in both plain text and HTML formats for better compatibility.

## Security Notes

- Never commit email passwords to version control
- Use app-specific passwords when available (recommended for Gmail)
- Consider using environment variable files (`.env`) for local development
- Ensure SMTP credentials are securely stored in production environments

## Troubleshooting

### Common Issues

1. **Email not sending**: Check that `EMAIL_ENABLED` is set to `"true"`
2. **Authentication failed**: Verify email credentials and use app passwords when required
3. **SMTP connection failed**: Check SMTP server and port settings
4. **Emails in spam**: Configure SPF/DKIM records for your domain (if using custom email)

### Logs

Email sending status is logged with these messages:
- Success: `"Email sent successfully to [recipients]"`
- Disabled: `"Email not configured or disabled, skipping email notification"`
- Error: `"Error sending email: [error details]"`

## Testing

To test email functionality:

1. Configure all required environment variables
2. Trigger a deployment approval through Slack
3. Check application logs for email sending status
4. Verify recipients receive the notification email

## Disabling Email Notifications

To disable email notifications, either:
- Set `EMAIL_ENABLED="false"`
- Remove the `EMAIL_ENABLED` variable entirely
- Remove any of the required email variables (`EMAIL_FROM`, `EMAIL_PASSWORD`, `EMAIL_TO`)
