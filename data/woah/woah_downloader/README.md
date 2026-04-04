# OIE-WAHIS SharePoint Downloader

Automated downloader for `infur_{date}.xlsx` from the OIE-WAHIS SharePoint site.
Designed to run in Dagster — fully unattended after the first interactive login.

---

## Files

| File | Purpose |
|------|---------|
| `sharepoint_auth.py` | Handles Microsoft OAuth (device code flow, token caching) |
| `sharepoint_downloader.py` | Downloads files via SharePoint REST API |
| `dagster_wahis_job.py` | Dagster job + weekly schedule |
| `it_request.md` | IT request letter (use if device code flow is blocked) |

---

## Quick Start

### 1. Install dependencies

```bash
pip install msal requests openpyxl dagster dagster-webserver
```

### 2. First-time authentication

```bash
python sharepoint_downloader.py --download-dir ./downloads
```

You'll see something like:
```
════════════════════════════════════════════════════════
  ONE-TIME AUTHENTICATION REQUIRED
════════════════════════════════════════════════════════
  1. Open this URL in any browser:
       https://microsoft.com/devicelogin
  2. Enter this code when prompted:
       ABCD1234
  3. Sign in as: dwvalentine@ucsd.edu
  4. Approve the Duo push on your phone.
════════════════════════════════════════════════════════
```

Open the URL, enter the code, approve Duo — script continues automatically.
**This only happens once (or every ~90 days).**

Token is cached at: `~/.wahis_token_cache.json`

### 3. Run in Dagster

```bash
dagster dev -f dagster_wahis_job.py
```

Open http://localhost:3000, then:
- Trigger `wahis_download_job` manually to test
- Enable the `wahis_weekly_schedule` for automatic Monday 6am runs

---

## CLI Reference

```bash
# Download latest infur file
python sharepoint_downloader.py

# Also download metadata file
python sharepoint_downloader.py --metadata

# List all files in the SharePoint folder (no download)
python sharepoint_downloader.py --list-only

# Force re-authentication (after 90 days or if token is revoked)
python sharepoint_downloader.py --reauth

# Specify download location
python sharepoint_downloader.py --download-dir /data/wahis
```

---

## Token Expiry

The refresh token is valid for **~90 days** (Microsoft O365 default).

After 90 days, the next Dagster run will fail with an authentication error.
To fix: run manually once on any machine with browser access:

```bash
python sharepoint_downloader.py --reauth
```

Then copy the updated `~/.wahis_token_cache.json` to your Dagster server.

**Set a calendar reminder every 80 days** as a buffer.

---

## Troubleshooting

### `PermissionError: Access denied` on first run
UCSD IT may have disabled the public client (device code) flow.
→ Send `it_request.md` to UCSD ITS to request an app registration.
→ Then update `sharepoint_auth.py` to use confidential client flow (see below).

### `Authentication failed: AADSTS70011`
The scope is wrong for this tenant. Edit `SCOPES` in `sharepoint_auth.py`:
```python
# Try this instead:
SCOPES = ["https://graph.microsoft.com/Sites.Read.All"]
```

### Files download as 0 bytes or HTML
The download URL format may need adjustment for this tenant.
Run `--list-only` first to confirm files are visible, then check the
`ServerRelativeUrl` values match what's expected.

---

## If IT Grants an App Registration (Path B)

Replace `sharepoint_auth.py` with this confidential client version:

```python
import msal

CLIENT_ID = "YOUR_CLIENT_ID"        # from IT
CLIENT_SECRET = "YOUR_SECRET"       # from IT (store in env var)
TENANT_ID = "YOUR_TENANT_ID"        # from IT
SCOPES = ["https://oieoffice365.sharepoint.com/.default"]

def get_token(force_reauth=False) -> str:
    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    )
    result = app.acquire_token_for_client(scopes=SCOPES)
    if "access_token" not in result:
        raise RuntimeError(result.get("error_description"))
    return result["access_token"]
```

With this version: **no device code, no Duo, no token expiry concerns.**
Fully unattended indefinitely. This is the production-grade solution.

Store credentials as environment variables:
```bash
export WAHIS_CLIENT_ID="..."
export WAHIS_CLIENT_SECRET="..."
export WAHIS_TENANT_ID="..."
```
