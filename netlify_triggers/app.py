import os
import logging
import requests
import hmac
import hashlib
from threading import Thread
from flask import Flask, request, jsonify
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

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


def send_preview_notification(channel, asset_name=None, metadata=None):
    """Send preview deployment notification with approval buttons"""
    try:
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
                    "text": f"Preview URL: <{NETLIFY_PREVIEW_URL}|View Preview>"
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
                        "value": metadata or "deploy"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "❌ Reject"
                        },
                        "style": "danger",
                        "action_id": "reject_deploy",
                        "value": metadata or "deploy"
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

        logger.info(f"Webhook received for asset: {asset_name}")

        # Send preview notification
        send_preview_notification(
            SLACK_CHANNEL_ID,
            asset_name=asset_name,
            metadata=metadata
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

        logger.info(f"Deploy approved by user {user_id}")

        # Trigger production build
        success = trigger_netlify_build(NETLIFY_PRODUCTION_HOOK)

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
                            "text": f"Preview URL: <{NETLIFY_PREVIEW_URL}|View Preview>"
                        }
                    }
                ]
            )

            # Post follow-up message with production URL
            client.chat_postMessage(
                channel=channel,
                text=f"🚀 Production deployment triggered! View at: {NETLIFY_PRODUCTION_URL}",
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"🚀 *Production deployment triggered!*\n\nProduction URL: <{NETLIFY_PRODUCTION_URL}|View Production>"
                        }
                    }
                ]
            )
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


@slack_app.action("reject_deploy")
def handle_reject_deploy(ack, body, client):
    """Handle reject button click"""
    ack()

    try:
        # Get message details
        channel = body["channel"]["id"]
        message_ts = body["message"]["ts"]
        user_id = body["user"]["id"]

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
                        "text": f"Preview URL: <{NETLIFY_PREVIEW_URL}|View Preview>"
                    }
                }
            ]
        )

        # Post follow-up message with rejection instructions
        client.chat_postMessage(
            channel=channel,
            text=NETLIFY_REJECT_MESSAGE,
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"ℹ️ {NETLIFY_REJECT_MESSAGE}"
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
                            "style": "primary"
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

        logger.info(f"Preview re-trigger requested by user {user_id}")

        # Trigger preview build
        success = trigger_netlify_build(NETLIFY_PREVIEW_HOOK)

        if success:
            # Update message to disable button
            client.chat_update(
                channel=channel,
                ts=message_ts,
                text=NETLIFY_REJECT_MESSAGE,
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"ℹ️ {NETLIFY_REJECT_MESSAGE}"
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

            # Send new preview notification
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