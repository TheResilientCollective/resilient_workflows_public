import os
import logging
import requests
import hmac
import hashlib
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from threading import Thread
from flask import Flask, request, jsonify
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler


from git import Repo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app for webhook endpoint
flask_app = Flask(__name__)

# Initialize Slack Bolt app
slack_app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# Configuration from environment variables
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID")
NETLIFY_PREVIEW_URL = os.environ.get("NETLIFY_PREVIEW_URL")
NETLIFY_PRODUCTION_URL = os.environ.get("NETLIFY_PRODUCTION_URL")
NETLIFY_PREVIEW_HOOK = os.environ.get("NETLIFY_PREVIEW_HOOK")
NETLIFY_PRODUCTION_HOOK = os.environ.get("NETLIFY_PRODUCTION_HOOK")
NETLIFY_REJECT_MESSAGE = os.environ.get("NETLIFY_REJECT_MESSAGE", "Please edit the prompts in Airtable and trigger a new preview when ready.")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
WEBHOOK_PORT = int(os.environ.get("WEBHOOK_PORT", 5000))

# Email configuration
EMAIL_SMTP_SERVER = os.environ.get("EMAIL_SMTP_SERVER", "smtp.gmail.com")
EMAIL_SMTP_PORT = int(os.environ.get("EMAIL_SMTP_PORT", 587))
EMAIL_FROM = os.environ.get("EMAIL_FROM")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")
EMAIL_TO = os.environ.get("EMAIL_TO")  # Can be comma-separated for multiple recipients
EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"


def verify_webhook_signature(payload, signature):
    """Verify webhook signature for security"""
    if not WEBHOOK_SECRET:
        return True  # Skip verification if no secret is set

    expected_signature = hmac.new(
        WEBHOOK_SECRET.encode(),
        payload.encode(),
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(signature, expected_signature)


def send_preview_notification(channel, asset_name=None, metadata=None,
                             preview_hook=None, deploy_hook=None,
                             preview_url=None, deploy_url=None,
                             reject_message=None, rt_url: str = None):
    """Send preview deployment notification with approval buttons"""
    try:
        # Use provided values or fall back to environment defaults
        preview_hook = preview_hook or NETLIFY_PREVIEW_HOOK
        deploy_hook = deploy_hook or NETLIFY_PRODUCTION_HOOK
        preview_url = preview_url or NETLIFY_PREVIEW_URL
        deploy_url = deploy_url or NETLIFY_PRODUCTION_URL
        reject_message = reject_message or NETLIFY_REJECT_MESSAGE
        rate_of_transmission_url = rt_url

        # Create state object to store in button values
        state = {
            "asset_name": asset_name,
            "metadata": metadata,
            "preview_hook": preview_hook,
            "deploy_hook": deploy_hook,
            "preview_url": preview_url,
            "deploy_url": deploy_url,
            "reject_message": reject_message,
            "rate_of_transmission_url": rate_of_transmission_url
        }
        state_json = json.dumps(state)

        message_text = "🚀 *Preview Deployment Ready*"
        if asset_name:
            message_text += f"\n📦 Asset: `{asset_name}`"

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message_text
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Preview URL: <{preview_url}|View Preview>"
                }
            },
            {
                "type": "actions",
                "block_id": "approval_actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "✅ Approve"
                        },
                        "style": "primary",
                        "action_id": "approve_deploy",
                        "value": state_json
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "❌ Reject"
                        },
                        "style": "danger",
                        "action_id": "reject_deploy",
                        "value": state_json
                    }
                ]
            }
        ]

        result = slack_app.client.chat_postMessage(
            channel=channel,
            text=message_text,
            blocks=blocks
        )

        logger.info(f"Preview notification sent to channel {channel}")
        return result

    except Exception as e:
        logger.error(f"Error sending preview notification: {str(e)}")
        raise


def send_email(subject, body, html_body=None):
    """Send email notification"""
    if not EMAIL_ENABLED or not EMAIL_FROM or not EMAIL_PASSWORD or not EMAIL_TO:
        logger.info("Email not configured or disabled, skipping email notification")
        return False

    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = EMAIL_FROM
        msg['Subject'] = subject

        # Handle multiple recipients
        recipients = [email.strip() for email in EMAIL_TO.split(',')]
        msg['To'] = ', '.join(recipients)

        # Add text body
        msg.attach(MIMEText(body, 'plain'))

        # Add HTML body if provided
        if html_body:
            msg.attach(MIMEText(html_body, 'html'))

        # Send email
        with smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.send_message(msg, to_addrs=recipients)

        logger.info(f"Email sent successfully to {', '.join(recipients)}")
        return True

    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")
        return False


def trigger_netlify_build(hook_url):
    """Trigger Netlify build via webhook"""
    try:
        response = requests.post(hook_url, json={}, timeout=10)
        response.raise_for_status()
        logger.info(f"Netlify build triggered successfully: {hook_url}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error triggering Netlify build: {str(e)}")
        return False


@flask_app.route('/webhook/trigger-preview', methods=['POST'])
def webhook_trigger_preview():
    """Webhook endpoint to receive Dagster triggers"""
    try:
        # Verify signature if secret is set
        signature = request.headers.get('X-Webhook-Signature', '')
        payload = request.get_data(as_text=True)

        if not verify_webhook_signature(payload, signature):
            logger.warning("Invalid webhook signature")
            return jsonify({"error": "Invalid signature"}), 401

        # Parse payload
        data = request.get_json()
        asset_name = data.get('asset_name')
        metadata = data.get('metadata')
        preview_hook =data.get("preview_hook") or NETLIFY_PREVIEW_HOOK
        deploy_hook =data.get("deploy_hook") or NETLIFY_PRODUCTION_HOOK
        preview_url =data.get("preview_url") or NETLIFY_PREVIEW_URL
        deploy_url =data.get("deploy_url") or NETLIFY_PRODUCTION_URL
        reject_message =data.get("reject_message") or NETLIFY_REJECT_MESSAGE

        logger.info(f"Webhook received for asset: {asset_name}")

        # Send preview notification with dynamic values
        send_preview_notification(
            SLACK_CHANNEL_ID,
            asset_name=asset_name,
            metadata=metadata,
            preview_hook=preview_hook,
            deploy_hook=deploy_hook,
            preview_url=preview_url,
            deploy_url=deploy_url,
            reject_message=reject_message
        )

        return jsonify({"status": "success", "message": "Preview notification sent"}), 200

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200


@slack_app.action("approve_deploy")
def handle_approve_deploy(ack, body, client):
    """Handle approve button click"""
    ack()

    try:
        # Get message details
        channel = body["channel"]["id"]
        message_ts = body["message"]["ts"]
        user_id = body["user"]["id"]

        # Extract state from button value
        button_value = body["actions"][0]["value"]
        try:
            state = json.loads(button_value)
            deploy_hook = state.get("deploy_hook", NETLIFY_PRODUCTION_HOOK)
            preview_url = state.get("preview_url", NETLIFY_PREVIEW_URL)
            deploy_url = state.get("deploy_url", NETLIFY_PRODUCTION_URL)
        except (json.JSONDecodeError, KeyError):
            # Fallback to environment defaults if state parsing fails
            logger.warning("Failed to parse state from button value, using defaults")
            deploy_hook = NETLIFY_PRODUCTION_HOOK
            preview_url = NETLIFY_PREVIEW_URL
            deploy_url = NETLIFY_PRODUCTION_URL

        logger.info(f"Deploy approved by user {user_id}")

        # Trigger production build with dynamic hook
        success = trigger_netlify_build(deploy_hook)

        if success:
            # Update original message to remove buttons
            client.chat_update(
                channel=channel,
                ts=message_ts,
                text="✅ Deployment Approved",
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"✅ *Deployment Approved* by <@{user_id}>"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"Preview URL: <{preview_url}|View Preview>"
                        }
                    }
                ]
            )

            # Post follow-up message with production URL
            client.chat_postMessage(
                channel=channel,
                text=f"🚀 Production deployment triggered! View at: {deploy_url}",
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"🚀 *Production deployment triggered!*\n\nProduction URL: <{deploy_url}|View Production>"
                        }
                    }
                ]
            )

            # Send email notification about deployment approval
            email_subject = "Seasonal virus forecast"
            email_body = f"""Dear all,

Our automated forecast with this week’s numbers is in the simplified portal at {deploy_url}

The production build has been successfully triggered and should be live shortly.
Best wishes,

Ruy

This is an automated notification from the Resilient Workflows system."""

            email_html = f"""
<!DOCTYPE html>
<html>
<body>

    <p>Dear all,</p>
<p>Our automated forecast with this week’s numbers is readyt</p>
   
    <ul>
        <li>The simplified portal at <a href="{deploy_url}">{deploy_url}</a></li>
    </ul>

    <p>Best wishes,.</p>
 <p>Ruy</p>
    <hr>
    <small>This is an automated notification from the Resilient Workflows system.</small>
</body>
</html>"""

            # Send email notification
            send_email(email_subject, email_body, email_html)
        else:
            # Post error message
            client.chat_postMessage(
                channel=channel,
                text="❌ Failed to trigger production deployment. Please check logs.",
                thread_ts=message_ts
            )

    except Exception as e:
        logger.error(f"Error handling approve action: {str(e)}")
        client.chat_postMessage(
            channel=channel,
            text=f"❌ Error: {str(e)}",
            thread_ts=message_ts
        )

# def copy_rt_to_github(rt_url: str, github_url: str):
#     # --- CONFIG ---
#     bucket_name = "your-bucket"
#     s3_key = "path/to/file.txt"
#     local_repo_path = "/tmp/myrepo"
#     local_file_path = os.path.join(local_repo_path, "file.txt")
#     commit_message = "Add file from S3"
#
#     # --- STEP 1: Download from S3 ---
#     s3 = boto3.client("s3")
#     s3.download_file(bucket_name, s3_key, local_file_path)
#
#     # --- STEP 2: Git operations ---
#     # Repo should already be cloned locally; if not, clone it first
#     # Repo.clone_from("https://github.com/username/repo.git", local_repo_path)
#
#     repo = Repo(local_repo_path)
#     repo.index.add([local_file_path])
#     repo.index.commit(commit_message)
#
#     # --- STEP 3: Push to GitHub ---
#     origin = repo.remote(name="origin")
#     origin.push()


@slack_app.action("reject_deploy")
def handle_reject_deploy(ack, body, client):
    """Handle reject button click"""
    ack()

    try:
        # Get message details
        channel = body["channel"]["id"]
        message_ts = body["message"]["ts"]
        user_id = body["user"]["id"]

        # Extract state from button value
        button_value = body["actions"][0]["value"]
        try:
            state = json.loads(button_value)
            preview_url = state.get("preview_url", NETLIFY_PREVIEW_URL)
            reject_message = state.get("reject_message", NETLIFY_REJECT_MESSAGE)
            preview_hook = state.get("preview_hook", NETLIFY_PREVIEW_HOOK)
        except (json.JSONDecodeError, KeyError):
            # Fallback to environment defaults if state parsing fails
            logger.warning("Failed to parse state from button value, using defaults")
            preview_url = NETLIFY_PREVIEW_URL
            reject_message = NETLIFY_REJECT_MESSAGE
            preview_hook = NETLIFY_PREVIEW_HOOK

        logger.info(f"Deploy rejected by user {user_id}")

        # Update original message to remove buttons
        client.chat_update(
            channel=channel,
            ts=message_ts,
            text="❌ Deployment Rejected",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"❌ *Deployment Rejected* by <@{user_id}>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Preview URL: <{preview_url}|View Preview>"
                    }
                }
            ]
        )

        # Create state for the trigger preview button
        trigger_state = {
            "preview_hook": preview_hook,
            "reject_message": reject_message
        }
        trigger_state_json = json.dumps(trigger_state)

        # Post follow-up message with rejection instructions
        client.chat_postMessage(
            channel=channel,
            text=reject_message,
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"ℹ️ {reject_message}"
                    }
                },
                {
                    "type": "actions",
                    "block_id": "retrigger_actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "🔄 Trigger Preview"
                            },
                            "action_id": "trigger_preview",
                            "style": "primary",
                            "value": trigger_state_json
                        }
                    ]
                }
            ]
        )

    except Exception as e:
        logger.error(f"Error handling reject action: {str(e)}")
        client.chat_postMessage(
            channel=channel,
            text=f"❌ Error: {str(e)}",
            thread_ts=message_ts
        )


@slack_app.action("trigger_preview")
def handle_trigger_preview(ack, body, client):
    """Handle trigger preview button click"""
    ack()

    try:
        # Get message details
        channel = body["channel"]["id"]
        message_ts = body["message"]["ts"]
        user_id = body["user"]["id"]

        # Extract state from button value
        button_value = body["actions"][0]["value"]
        try:
            state = json.loads(button_value)
            preview_hook = state.get("preview_hook", NETLIFY_PREVIEW_HOOK)
            reject_message = state.get("reject_message", NETLIFY_REJECT_MESSAGE)
        except (json.JSONDecodeError, KeyError):
            # Fallback to environment defaults if state parsing fails
            logger.warning("Failed to parse state from button value, using defaults")
            preview_hook = NETLIFY_PREVIEW_HOOK
            reject_message = NETLIFY_REJECT_MESSAGE

        logger.info(f"Preview re-trigger requested by user {user_id}")

        # Trigger preview build with dynamic hook
        success = trigger_netlify_build(preview_hook)

        if success:
            # Update message to disable button
            client.chat_update(
                channel=channel,
                ts=message_ts,
                text=reject_message,
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"ℹ️ {reject_message}"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"✅ *Preview build triggered* by <@{user_id}>"
                        }
                    }
                ]
            )

            # Send new preview notification - use defaults since we don't have full state
            send_preview_notification(channel)
        else:
            # Post error message
            client.chat_postMessage(
                channel=channel,
                text="❌ Failed to trigger preview build. Please check logs.",
                thread_ts=message_ts
            )

    except Exception as e:
        logger.error(f"Error handling trigger preview action: {str(e)}")
        client.chat_postMessage(
            channel=channel,
            text=f"❌ Error: {str(e)}",
            thread_ts=message_ts
        )


@slack_app.event("message")
def handle_message_events(body, say):
    """Handle general message events"""
    # Log the message for debugging but don't respond
    # This prevents the "unhandled request" error
    logger.info(f"Received message event: {body.get('event', {}).get('type', 'unknown')}")
    # Optionally, you can add logic here to respond to specific messages
    # For now, we just acknowledge the event to prevent errors


def run_flask():
    """Run Flask app in a separate thread"""
    logger.info(f"Starting Flask webhook server on port {WEBHOOK_PORT}")
    flask_app.run(host='0.0.0.0', port=WEBHOOK_PORT, debug=False)


def run_slack():
    """Run Slack Bolt app with Socket Mode"""
    logger.info("Starting Slack Bolt app with Socket Mode")
    handler = SocketModeHandler(slack_app, os.environ["SLACK_APP_TOKEN"])
    handler.start()


if __name__ == "__main__":
    logger.info("Starting Netlify Triggers Slack App")

    # Validate required environment variables
    required_vars = [
        "SLACK_BOT_TOKEN",
        "SLACK_APP_TOKEN",
        "SLACK_CHANNEL_ID",
        "NETLIFY_PREVIEW_URL",
        "NETLIFY_PRODUCTION_URL",
        "NETLIFY_PREVIEW_HOOK",
        "NETLIFY_PRODUCTION_HOOK"
    ]

    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        exit(1)

    # Start Flask in a separate thread
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Start Slack Bolt app (blocks main thread)
    run_slack()
