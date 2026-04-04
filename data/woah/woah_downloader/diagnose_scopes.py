"""
diagnose_scopes.py
==================
Tries different OAuth scope/token combinations to find what works
for oieoffice365.sharepoint.com.

Run this first to identify the right scope, then update sharepoint_auth.py.
"""

import json
import sys
from pathlib import Path

import msal
import requests

# ── Try these scopes in order ─────────────────────────────────────────────────
SCOPE_CANDIDATES = [
    # Most common for SharePoint REST API
    ["https://oieoffice365.sharepoint.com/AllSites.Read"],
    ["https://oieoffice365.sharepoint.com/AllSites.FullControl"],
    # Graph API approach (alternative path)
    ["https://graph.microsoft.com/Sites.Read.All"],
    ["https://graph.microsoft.com/Files.Read.All"],
    # Generic .default (uses whatever the app was consented for)
    ["https://oieoffice365.sharepoint.com/.default"],
    ["https://graph.microsoft.com/.default"],
]

CLIENT_ID = "d3590ed6-52b3-4102-aeff-aad2292ab01c"  # Microsoft Office public client
USER_EMAIL = "dwvalentine@ucsd.edu"
TOKEN_CACHE_PATH = Path.home() / ".wahis_token_cache.json"

SHAREPOINT_HOST = "oieoffice365.sharepoint.com"
SITE_NAME = "PeriodicaldataextractionsOIE-WAHIS"
FOLDER_PATH = "Shared Documents"


def load_cache():
    cache = msal.SerializableTokenCache()
    if TOKEN_CACHE_PATH.exists():
        cache.deserialize(TOKEN_CACHE_PATH.read_text())
    return cache


def save_cache(cache):
    if cache.has_state_changed:
        TOKEN_CACHE_PATH.write_text(cache.serialize())


def get_token_for_scope(scopes: list[str]) -> str | None:
    cache = load_cache()
    app = msal.PublicClientApplication(
        client_id=CLIENT_ID,
        authority="https://login.microsoftonline.com/common",
        token_cache=cache,
    )

    # Try silent first
    accounts = app.get_accounts(username=USER_EMAIL)
    if accounts:
        result = app.acquire_token_silent(scopes, account=accounts[0])
        if result and "access_token" in result:
            save_cache(cache)
            return result["access_token"]

    # If silent fails, do device code
    print(f"\n  Silent auth failed for scope: {scopes}")
    print("  Starting device code flow...")
    flow = app.initiate_device_flow(scopes=scopes)
    if "user_code" not in flow:
        print(f"  ✗ Could not initiate flow: {flow.get('error_description')}")
        return None

    print(f"\n  → Go to: {flow['verification_uri']}")
    print(f"  → Enter code: {flow['user_code']}")
    print(f"  → Sign in as {USER_EMAIL} and approve Duo")
    print(f"  Waiting...")

    result = app.acquire_token_by_device_flow(flow)
    save_cache(cache)

    if "access_token" in result:
        return result["access_token"]
    else:
        print(f"  ✗ Auth failed: {result.get('error_description')}")
        return None


def test_sharepoint_rest(token: str) -> bool:
    """Test SharePoint REST API directly."""
    url = f"https://{SHAREPOINT_HOST}/sites/{SITE_NAME}/_api/web?$select=Url"
    r = requests.get(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;odata=verbose",
    }, timeout=15)
    print(f"    SharePoint REST /_api/web → {r.status_code}")
    if r.status_code == 200:
        print(f"    Site URL: {r.json()['d']['Url']}")
        return True
    return False


def test_sharepoint_files(token: str) -> bool:
    """Test listing files in the folder."""
    folder = f"/sites/{SITE_NAME}/{FOLDER_PATH}"
    encoded = folder.replace(" ", "%20")
    url = (
        f"https://{SHAREPOINT_HOST}/sites/{SITE_NAME}/_api/web"
        f"/GetFolderByServerRelativeUrl('{encoded}')/Files"
        f"?$select=Name,TimeLastModified&$top=5"
    )
    r = requests.get(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;odata=verbose",
    }, timeout=15)
    print(f"    SharePoint REST /Files → {r.status_code}")
    if r.status_code == 200:
        files = r.json()["d"]["results"]
        print(f"    Files found: {len(files)}")
        for f in files[:3]:
            print(f"      - {f['Name']}")
        return True
    return False


def test_graph_site(token: str) -> bool:
    """Test Microsoft Graph API for SharePoint."""
    # First get the site ID
    url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_HOST}:/sites/{SITE_NAME}"
    r = requests.get(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }, timeout=15)
    print(f"    Graph /sites → {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"    Site: {data.get('displayName')} (id: {data.get('id', '')[:20]}...)")
        return True
    return False


def test_graph_files(token: str) -> bool:
    """Test listing files via Graph drive API."""
    # Get site
    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_HOST}:/sites/{SITE_NAME}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    if r.status_code != 200:
        return False
    site_id = r.json()["id"]

    # List drive items
    r2 = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    print(f"    Graph /drive/root/children → {r2.status_code}")
    if r2.status_code == 200:
        items = r2.json().get("value", [])
        print(f"    Items found: {len(items)}")
        for item in items[:3]:
            print(f"      - {item['name']}")
        return True
    return False


def main():
    print("=" * 65)
    print("  SharePoint Scope Diagnostics")
    print(f"  Target: https://{SHAREPOINT_HOST}/sites/{SITE_NAME}")
    print("=" * 65)

    working = []

    for scopes in SCOPE_CANDIDATES:
        print(f"\n{'─'*65}")
        print(f"Testing scope: {scopes[0]}")

        token = get_token_for_scope(scopes)
        if not token:
            print("  ✗ Could not obtain token")
            continue

        print(f"  ✓ Token obtained")

        # Determine which tests to run based on scope type
        is_graph = "graph.microsoft.com" in scopes[0]

        if is_graph:
            site_ok = test_graph_site(token)
            files_ok = test_graph_files(token) if site_ok else False
        else:
            site_ok = test_sharepoint_rest(token)
            files_ok = test_sharepoint_files(token) if site_ok else False

        if site_ok and files_ok:
            print(f"  ✓✓ FULL SUCCESS with scope: {scopes[0]}")
            working.append({
                "scope": scopes[0],
                "api": "graph" if is_graph else "sharepoint_rest",
            })
        elif site_ok:
            print(f"  ~ Partial: site accessible but files failed")
        else:
            print(f"  ✗ Neither test passed")

    print(f"\n{'='*65}")
    if working:
        print(f"WORKING SCOPE(S) FOUND:")
        for w in working:
            print(f"  Scope: {w['scope']}")
            print(f"  API:   {w['api']}")
        best = working[0]
        print(f"\nRECOMMENDATION: Update sharepoint_auth.py:")
        print(f"  SCOPES = [\"{best['scope']}\"]")
        if best["api"] == "graph":
            print(f"  Also update sharepoint_downloader.py to use Graph API endpoints.")
            print(f"  (Run: python graph_downloader.py after getting this working)")
    else:
        print("NO WORKING SCOPE FOUND.")
        print("UCSD IT has likely restricted public client app flows.")
        print("→ Proceed with it_request.md to get an app registration.")

    return working


if __name__ == "__main__":
    main()
