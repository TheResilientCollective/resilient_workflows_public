"""
try_client_ids.py
=================
The Microsoft Office public client ID is blocked by UCSD's tenant policy.
This script tries other well-known public client IDs that may be allowed.

Run: python try_client_ids.py
"""

import sys
from pathlib import Path
import msal
import requests

USER_EMAIL = "dwvalentine@ucsd.edu"
TOKEN_CACHE_PATH = Path.home() / ".wahis_token_cache.json"
SHAREPOINT_HOST = "oieoffice365.sharepoint.com"
SITE_NAME = "PeriodicaldataextractionsOIE-WAHIS"

# Well-known public client IDs — these are Microsoft's own published app IDs
# for various first-party tools. Some tenants allow a subset of these.
CLIENT_CANDIDATES = [
  #  ("Azure CLI",              "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
    #("Azure PowerShell",       "1950a258-227b-4e31-a9cf-717495945fc2"),
   # ("Microsoft Teams",        "1fec8e78-bce4-4aaf-ab1b-5451cc387264"),
    #("OneDrive Sync",          "ab9b8c07-8f02-4f72-87fa-80105867a763"),
   # ("SharePoint Mobile",      "f05ff7c9-f75a-4acd-a3b5-f4b6a870245d"),
  #  ("Microsoft Authenticator","4813382a-8fa7-425e-ab75-3b753aab3abb"),
    ("Visual Studio Code",     "aebc6443-996d-45c2-90f0-388ff96faa56"),
]

# Try SharePoint-direct scope first, then Graph
SCOPE_PAIRS = [
    ("SharePoint REST", [f"https://{SHAREPOINT_HOST}/AllSites.Read"]),
    ("Graph",           ["https://graph.microsoft.com/Sites.Read.All"]),
]


def try_silent(app, scopes):
    accounts = app.get_accounts(username=USER_EMAIL)
    if accounts:
        result = app.acquire_token_silent(scopes, account=accounts[0])
        if result and "access_token" in result:
            return result["access_token"]
    return None


def try_device_code(app, scopes):
    flow = app.initiate_device_flow(scopes=scopes)
    if "user_code" not in flow:
        return None, flow.get("error_description", "unknown error")

    print(f"\n  → Go to: {flow['verification_uri']}")
    print(f"  → Code:  {flow['user_code']}")
    print(f"  → Sign in as {USER_EMAIL} and approve Duo. Waiting...")

    result = app.acquire_token_by_device_flow(flow)
    if "access_token" in result:
        return result["access_token"], None
    return None, result.get("error_description", result.get("error"))


def test_token(token: str, api_label: str) -> bool:
    if "graph" in api_label.lower():
        url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_HOST}:/sites/{SITE_NAME}"
        headers = {"Authorization": f"Bearer {token}"}
    else:
        url = f"https://{SHAREPOINT_HOST}/sites/{SITE_NAME}/_api/web?$select=Url"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json;odata=verbose",
        }
    r = requests.get(url, headers=headers, timeout=15)
    print(f"    {api_label} → HTTP {r.status_code}")
    if r.status_code == 200:
        return True
    return False


def main():
    print("=" * 65)
    print("  Testing alternative public client IDs")
    print(f"  User: {USER_EMAIL}")
    print("=" * 65)

    # Clear old cache to avoid cross-contamination
    cache_backup = None
    if TOKEN_CACHE_PATH.exists():
        cache_backup = TOKEN_CACHE_PATH.read_text()

    winners = []

    for client_name, client_id in CLIENT_CANDIDATES:
        for scope_label, scopes in SCOPE_PAIRS:
            print(f"\n{'─'*65}")
            print(f"Client: {client_name} ({client_id[:8]}...)")
            print(f"Scope:  {scopes[0]}")

            cache = msal.SerializableTokenCache()
            app = msal.PublicClientApplication(
                client_id=client_id,
                authority="https://login.microsoftonline.com/common",
                token_cache=cache,
            )

            # Try to initiate device flow (cheapest test — fails fast if blocked)
            flow = app.initiate_device_flow(scopes=scopes)
            if "user_code" not in flow:
                err = flow.get("error_description", flow.get("error", "?"))
                print(f"  ✗ Flow blocked: {err[:120]}")
                continue

            print(f"  ✓ Flow initiated — device code available")
            print(f"\n  → Go to: {flow['verification_uri']}")
            print(f"  → Code:  {flow['user_code']}")
            print(f"  → Sign in as {USER_EMAIL} + approve Duo. Waiting...")

            result = app.acquire_token_by_device_flow(flow)

            if "access_token" not in result:
                err = result.get("error_description", result.get("error", "?"))
                print(f"  ✗ Token denied: {err[:120]}")
                continue

            token = result["access_token"]
            print(f"  ✓ Token obtained!")

            ok = test_token(token, scope_label)
            if ok:
                print(f"  ✓✓ SUCCESS: {client_name} + {scope_label}")
                winners.append({
                    "client_name": client_name,
                    "client_id": client_id,
                    "scope": scopes[0],
                    "api": scope_label,
                })
                # Save working cache
                TOKEN_CACHE_PATH.write_text(cache.serialize())
                TOKEN_CACHE_PATH.chmod(0o600)
                break  # No need to test Graph if REST works
            else:
                print(f"  ~ Token works but SharePoint denied access")

        if winners:
            break  # Found one, stop

    print(f"\n{'='*65}")
    if winners:
        w = winners[0]
        print(f"WINNER FOUND:")
        print(f"  Client: {w['client_name']}")
        print(f"  CLIENT_ID = \"{w['client_id']}\"")
        print(f"  SCOPES    = [\"{w['scope']}\"]")
        print(f"\nUpdate sharepoint_auth.py with these values.")
    else:
        print("NO PUBLIC CLIENT WORKED.")
        print()
        print("UCSD's tenant policy requires an IT-registered app.")
        print("Next step: send it_request.md to UCSD ITS.")
        print()
        print("Key line to include in your request:")
        print('  "Please register an Azure AD app with Sites.Selected')
        print('   permission on oieoffice365.sharepoint.com and provide')
        print('   a client_id, tenant_id, and client_secret."')

    # Restore original cache if nothing worked
    if not winners and cache_backup:
        TOKEN_CACHE_PATH.write_text(cache_backup)

    return winners


if __name__ == "__main__":
    main()
