"""
sharepoint_auth.py
==================
Handles Microsoft authentication via Device Code Flow.

- First run: prints a code, you approve once (with Duo), token cached locally
- Subsequent runs: uses cached refresh token silently (valid ~90 days)
- Works without IT / app registration using Microsoft's well-known public client ID

Usage:
    from sharepoint_auth import get_token
    token = get_token()  # silent if cache valid, prompts if not
"""

import json
import sys
from pathlib import Path

import msal

# ── Constants ────────────────────────────────────────────────────────────────

# Microsoft Office public client ID — this is Microsoft's own published ID,
# used by the official Office CLI tools. No app registration needed.
# See: https://learn.microsoft.com/en-us/azure/active-directory/develop/reference-app-manifest
CLIENT_ID = "d3590ed6-52b3-4102-aeff-aad2292ab01c"  # Microsoft Office

TENANT = "common"  # Use 'common' for multi-tenant; works for UCSD O365

SCOPES = [
    "https://oieoffice365.sharepoint.com/.default",
]

# Where to persist the token cache (~/.wahis_token_cache.json)
TOKEN_CACHE_PATH = Path.home() / ".wahis_token_cache.json"

USER_EMAIL = "dwvalentine@ucsd.edu"


# ── Token cache helpers ───────────────────────────────────────────────────────

def _load_cache() -> msal.SerializableTokenCache:
    cache = msal.SerializableTokenCache()
    if TOKEN_CACHE_PATH.exists():
        cache.deserialize(TOKEN_CACHE_PATH.read_text())
    return cache


def _save_cache(cache: msal.SerializableTokenCache):
    if cache.has_state_changed:
        TOKEN_CACHE_PATH.write_text(cache.serialize())
        TOKEN_CACHE_PATH.chmod(0o600)  # owner read/write only


# ── Main auth function ────────────────────────────────────────────────────────

def get_token(force_reauth: bool = False) -> str:
    """
    Returns a valid Bearer token string.

    On first call (or if force_reauth=True):
        Prints a device code + URL. Open the URL, enter the code, approve Duo.
        Token is cached locally for ~90 days.

    On subsequent calls:
        Silently refreshes from cache — no human interaction needed.

    Raises:
        RuntimeError if authentication fails.
    """
    cache = _load_cache()

    app = msal.PublicClientApplication(
        client_id=CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT}",
        token_cache=cache,
    )

    # ── Try silent auth from cache first ──────────────────────────────────────
    if not force_reauth:
        accounts = app.get_accounts(username=USER_EMAIL)
        if accounts:
            result = app.acquire_token_silent(SCOPES, account=accounts[0])
            if result and "access_token" in result:
                _save_cache(cache)
                return result["access_token"]

    # ── Device code flow (interactive, once) ──────────────────────────────────
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Failed to initiate device flow: {flow.get('error_description')}")

    # Print clear instructions
    print()
    print("=" * 65)
    print("  ONE-TIME AUTHENTICATION REQUIRED")
    print("=" * 65)
    print(f"  1. Open this URL in any browser:")
    print(f"       {flow['verification_uri']}")
    print()
    print(f"  2. Enter this code when prompted:")
    print(f"       {flow['user_code']}")
    print()
    print(f"  3. Sign in as: {USER_EMAIL}")
    print(f"  4. Approve the Duo push on your phone.")
    print()
    print(f"  Waiting (up to {flow['expires_in'] // 60} minutes)...")
    print("=" * 65)

    result = app.acquire_token_by_device_flow(flow)  # blocks until done or timeout

    if "access_token" not in result:
        error = result.get("error_description", result.get("error", "unknown"))
        raise RuntimeError(f"Authentication failed: {error}")

    _save_cache(cache)
    print("  ✓ Authenticated successfully. Token cached.")
    print(f"  Cache: {TOKEN_CACHE_PATH}")
    return result["access_token"]


if __name__ == "__main__":
    # Quick test
    force = "--reauth" in sys.argv
    token = get_token(force_reauth=force)
    print(f"\nToken obtained (first 40 chars): {token[:40]}...")
