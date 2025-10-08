# Implementation Plan: Netlify Approval Workflow

## Overview
Build a Slack bot that manages an approval workflow for Netlify deployments, triggered by Dagster asset completion. The bot will handle preview deployments, approval/rejection actions, and production deploys via webhook triggers.

## Architecture Components

**1. Slack Bot (app.py)**
- Webhook endpoint to receive Dagster triggers
- Interactive message handlers for Approve/Reject buttons
- HTTP client to trigger Netlify build hooks
- State management for deployment tracking

**2. Configuration (Environment Variables)**
```
SLACK_BOT_TOKEN - Bot OAuth token
SLACK_APP_TOKEN - App-level token for Socket Mode
SLACK_CHANNEL_ID - Target channel for notifications

NETLIFY_PREVIEW_URL - URL of preview deployment
NETLIFY_PRODUCTION_URL - URL of production deployment
NETLIFY_PREVIEW_HOOK - Webhook to trigger preview deploy
NETLIFY_PRODUCTION_HOOK - Webhook to trigger production deploy
NETLIFY_REJECT_MESSAGE - Custom message shown on rejection
```

**3. Message Flow**

```
Dagster Asset → Webhook → Preview Notification
                              ↓
                    [Approve] [Reject]
                      ↓           ↓
              Production Deploy   Show Edit Message
              Disable buttons     [Trigger Preview]
              Success message           ↓
                                  Preview Notification (loop)
```

## Implementation Steps

**Step 1: Configuration & Setup**
- Update requirements.txt with dependencies (requests)
- Create .env.example with all required variables
- Update manifest.json with required scopes

**Step 2: Webhook Endpoint**
- Add Flask/FastAPI endpoint to receive Dagster triggers
- Parse incoming payload (asset name, metadata)
- Validate request authenticity
- Trigger initial preview notification

**Step 3: Preview Notification Handler**
- Create Slack message with preview URL
- Add "Approve" and "Reject" action buttons
- Include metadata (timestamp, asset name, deployment info)
- Store message_ts for future updates

**Step 4: Approve Action Handler**
- Acknowledge button click immediately
- POST to NETLIFY_PRODUCTION_HOOK
- Update original message (disable buttons)
- Post follow-up with production URL
- Log approval event

**Step 5: Reject Action Handler**
- Acknowledge button click
- Update message with rejection status
- Post custom rejection message (NETLIFY_REJECT_MESSAGE)
- Add "Trigger Preview" button
- Handle preview re-trigger

**Step 6: Error Handling**
- Webhook validation failures
- Netlify API errors (timeout, 4xx, 5xx)
- Slack API failures
- Logging and monitoring

**Step 7: Testing & Documentation**
- Unit tests for handlers
- Integration test with mock webhooks
- Update README with setup instructions
- Document environment variables

## Technical Considerations

**Slack App Permissions Required:**
- `chat:write` - Send messages
- `chat:write.customize` - Customize message appearance
- `channels:history` - Read channel messages
- `commands` - Slash commands (optional for manual triggers)

**Deployment Options:**
1. Socket Mode (current): Runs locally or on server
2. HTTP Mode: Requires public endpoint for Slack events
3. Consider switching to HTTP mode for webhook integration

**State Management:**
- Store deployment context in message metadata
- Use Slack's message metadata feature
- Consider Redis/database for complex workflows

**Security:**
- Verify Slack request signatures
- Validate Dagster webhook tokens
- Don't expose Netlify hooks in messages
- Rate limiting on endpoints