# Netlify Deployment Approval Workflow

A Slack bot that manages an approval workflow for Netlify deployments, triggered by Dagster asset completion. The bot handles preview deployments, approval/rejection actions, and production deploys via webhook triggers.

## Features

- 🚀 **Automated Preview Notifications**: Receive Slack notifications when Dagster assets complete
- ✅ **Approval Workflow**: Approve or reject deployments with interactive buttons
- 🔄 **Re-trigger Capability**: Easily re-trigger preview builds after making changes
- 🔒 **Webhook Security**: Optional HMAC signature verification for Dagster webhooks
- 📝 **Comprehensive Logging**: Detailed logging for debugging and monitoring
- 🎯 **Generic Design**: Configurable for multiple preview/production services

## Architecture

The application consists of two main components:

1. **Flask Webhook Server**: Receives triggers from Dagster and posts notifications to Slack
2. **Slack Bolt App**: Handles interactive button clicks and manages the approval workflow

Both run concurrently using threading.

## Workflow

```
Dagster Asset Completes
         ↓
    Webhook Trigger
         ↓
Preview Notification Sent
         ↓
   [Approve] [Reject]
      ↓           ↓
Production     Edit & Retry
   Deploy      [Trigger Preview]
      ↓              ↓
   Success    (Back to Preview)
```

## Setup

### 1. Install Dependencies

```bash
# Create virtual environment (if not already done)
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Create Slack App

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps)
2. Click "Create New App" → "From a manifest"
3. Choose your workspace
4. Upload or paste the contents of `manifest.json`
5. Click "Create"

### 3. Configure Slack App

#### Install to Workspace
1. Navigate to "Install App" in the sidebar
2. Click "Install to Workspace"
3. Authorize the app

#### Enable Socket Mode
1. Navigate to "Socket Mode" in the sidebar
2. Enable Socket Mode
3. Generate an App-Level Token with `connections:write` scope
4. Save the token (starts with `xapp-`)

#### Get Bot Token
1. Navigate to "OAuth & Permissions"
2. Copy the "Bot User OAuth Token" (starts with `xoxb-`)

#### Get Channel ID
1. Open Slack and go to the channel where you want notifications
2. Right-click the channel → "View channel details"
3. Scroll down to find the Channel ID

## add bot to channel
1. select a channel
2. go '@bot-name', eg '@netlify-triggers' to add bot
3. click "add them"

### 4. Get Netlify Webhooks

#### Create Build Hooks
1. Go to your Netlify site dashboard
2. Navigate to "Site settings" → "Build & deploy" → "Build hooks"
3. Create two hooks:
   - **Preview Hook**: For preview deployments
   - **Production Hook**: For production deployments
4. Copy the webhook URLs

#### Get Deployment URLs
- **Preview URL**: Your preview/staging deployment URL
- **Production URL**: Your production deployment URL

### 5. Configure Environment Variables

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your values
nano .env
```

Required variables:
```bash
# Slack Configuration
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_APP_TOKEN=xapp-your-app-token
SLACK_CHANNEL_ID=C0123456789

# Netlify Configuration
NETLIFY_PREVIEW_URL=https://preview.example.netlify.app
NETLIFY_PRODUCTION_URL=https://example.netlify.app
NETLIFY_PREVIEW_HOOK=https://api.netlify.com/build_hooks/your-preview-hook-id
NETLIFY_PRODUCTION_HOOK=https://api.netlify.com/build_hooks/your-production-hook-id

# Custom Messages
NETLIFY_REJECT_MESSAGE=Please edit the prompts in Airtable and trigger a new preview when ready.

# Webhook Configuration (optional)
#WEBHOOK_SECRET=your-secret-key-for-dagster-webhook
WEBHOOK_PORT=5000
```

### 6. Run the Application

```bash
# Load environment variables
source .env  # On Windows: set -a && source .env && set +a

# Run the app
python3 app.py
```

You should see:
```
Starting Netlify Triggers Slack App
Starting Flask webhook server on port 5000
Starting Slack Bolt app with Socket Mode
```

## Usage

### Trigger from Dagster

To trigger a preview notification from a Dagster asset, make an HTTP POST request to the webhook endpoint:

```python
import requests
import hmac
import hashlib

# Webhook configuration
webhook_url = "http://localhost:5000/webhook/trigger-preview"
webhook_secret = "your-secret-key"  # Must match WEBHOOK_SECRET

# Payload
payload = {
    "asset_name": "my_dagster_asset",
    "metadata": "optional-metadata"
}

# Generate signature (if using WEBHOOK_SECRET)
payload_str = str(payload)
signature = hmac.new(
    webhook_secret.encode(),
    payload_str.encode(),
    hashlib.sha256
).hexdigest()

# Make request
response = requests.post(
    webhook_url,
    json=payload,
    headers={"X-Webhook-Signature": signature}
)

print(response.json())
```

### Dagster Integration Example

Add this to your Dagster asset:

```python
from dagster import asset, OpExecutionContext
import requests

@asset
def my_asset(context: OpExecutionContext):
    # Your asset logic here
    ...

    # Trigger Slack notification
    context.log.info("Triggering Slack notification")
    response = requests.post(
        "http://localhost:5000/webhook/trigger-preview",
        json={"asset_name": context.asset_key.to_user_string()}
    )

    if response.ok:
        context.log.info("Slack notification sent successfully")
    else:
        context.log.warning(f"Failed to send Slack notification: {response.text}")

    return result
```

### Approval Workflow

1. **Preview Notification**: When triggered, a Slack message appears with:
   - Asset name (if provided)
   - Preview URL link
   - ✅ Approve and ❌ Reject buttons

2. **Approve**:
   - Triggers production deployment via Netlify webhook
   - Updates message to show approval status
   - Posts follow-up with production URL

3. **Reject**:
   - Updates message to show rejection status
   - Posts custom rejection message
   - Provides 🔄 Trigger Preview button to restart the workflow

## API Endpoints

### POST /webhook/trigger-preview

Trigger a preview notification.

**Headers:**
- `X-Webhook-Signature` (optional): HMAC SHA-256 signature of payload

**Body:**
```json
{
  "asset_name": "string (optional)",
  "metadata": "string (optional)"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Preview notification sent"
}
```

### GET /health

Health check endpoint.

**Response:**
```json
{
  "status": "healthy"
}
```

## Security

### Webhook Signature Verification

To secure the webhook endpoint, set `WEBHOOK_SECRET` in your environment variables. The app will verify incoming requests using HMAC SHA-256 signatures.

Generate signature in Python:
```python
import hmac
import hashlib

signature = hmac.new(
    webhook_secret.encode(),
    payload_string.encode(),
    hashlib.sha256
).hexdigest()
```

Include the signature in the `X-Webhook-Signature` header.

## Troubleshooting

### App won't start

**Error**: `Missing required environment variables`
- **Solution**: Ensure all required environment variables are set in `.env`

**Error**: `slack_bolt.error.BoltError: The token is invalid`
- **Solution**: Verify your `SLACK_BOT_TOKEN` is correct and starts with `xoxb-`

### Buttons not responding

**Error**: No response when clicking buttons
- **Solution**: Ensure Socket Mode is enabled and `SLACK_APP_TOKEN` is valid

### Netlify builds not triggering

**Error**: Builds don't start after approval
- **Solution**: Verify your Netlify build hook URLs are correct and accessible

### Webhook not receiving requests

**Error**: 401 Invalid signature
- **Solution**: Ensure `WEBHOOK_SECRET` matches on both client and server

**Error**: Connection refused
- **Solution**: Check that Flask server is running on the correct port

## Development

### Run with Debug Logging

```bash
# Set log level to DEBUG
export LOG_LEVEL=DEBUG
python3 app.py
```

### Test Webhook Locally

```bash
# Test without signature
curl -X POST http://localhost:5000/webhook/trigger-preview \
  -H "Content-Type: application/json" \
  -d '{"asset_name": "test_asset"}'

# Check health endpoint
curl http://localhost:5000/health
```

### Run Tests

```bash
# Install test dependencies
pip install pytest pytest-mock

# Run tests
pytest tests/
```

## Deployment

### Docker

#### Using Pre-built Image (Recommended)

Pull the latest image from Docker Hub:
```bash
# Pull latest image
docker pull resilientucsd/netlify-triggers:latest

# Or pull a specific branch
docker pull resilientucsd/netlify-triggers:master
docker pull resilientucsd/netlify-triggers:dev

# Run with environment file
docker run --env-file .env -p 5000:5000 resilientucsd/netlify-triggers:latest
```

#### Building Locally

Build and run using the included Dockerfile:
```bash
# Build image
docker build -t netlify-triggers .

# Run with environment file
docker run --env-file .env -p 5000:5000 netlify-triggers

# Or pass environment variables directly
docker run -e SLACK_BOT_TOKEN=xoxb-... \
  -e SLACK_APP_TOKEN=xapp-... \
  -e SLACK_CHANNEL_ID=C0123456789 \
  -e NETLIFY_PREVIEW_URL=https://preview.example.com \
  -e NETLIFY_PRODUCTION_URL=https://example.com \
  -e NETLIFY_PREVIEW_HOOK=https://api.netlify.com/... \
  -e NETLIFY_PRODUCTION_HOOK=https://api.netlify.com/... \
  -p 5000:5000 netlify-triggers
```

#### Docker Compose

Create a `docker-compose.yml`:
```yaml
version: '3.8'

services:
  netlify-triggers:
    image: resilientucsd/netlify-triggers:latest
    # Or build locally:
    # build: .
    ports:
      - "5000:5000"
    env_file:
      - .env
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 5s
```

Run with:
```bash
docker-compose up -d
```

#### CI/CD with GitHub Actions

The project includes automated Docker builds via GitHub Actions. Images are automatically built and pushed to Docker Hub when:
- Code is pushed to `master`, `dev`, or feature branches
- Changes are made to the `netlify_triggers/` directory
- Tagged releases are created (e.g., `netlify-v1.0.0`)

The workflow is defined in `.github/workflows/containerize_netlify_triggers.yaml`.

### Production Considerations

1. **Use HTTPS**: Deploy behind a reverse proxy (nginx, Caddy) with SSL
2. **Set WEBHOOK_SECRET**: Always use signature verification in production
3. **Monitor Logs**: Use log aggregation (ELK, Datadog, CloudWatch)
4. **Health Checks**: Configure load balancer to ping `/health`
5. **Rate Limiting**: Add rate limiting to webhook endpoints

## License

Apache License 2.0 - See LICENSE file for details

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Support

For issues and questions:
- Open an issue on GitHub
- Check existing issues for solutions
- Review logs for error messages
