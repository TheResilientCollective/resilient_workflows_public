"""
sharepoint_downloader.py
========================
Downloads the latest infur_{date}.xlsx (and optionally the metadata file)
from the OIE-WAHIS SharePoint site.

After the first interactive authentication (device code + Duo), all
subsequent runs are fully unattended — suitable for Dagster or cron.

Usage:
    # First run — will prompt for device code auth
    python sharepoint_downloader.py --download-dir ./downloads

    # Subsequent runs — fully silent
    python sharepoint_downloader.py --download-dir ./downloads

    # Force re-authentication (e.g. after 90 days)
    python sharepoint_downloader.py --reauth

    # Check what files exist without downloading
    python sharepoint_downloader.py --list-only
"""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import requests

from sharepoint_auth import get_token

# ── Configuration ─────────────────────────────────────────────────────────────

SHAREPOINT_HOST = "oieoffice365.sharepoint.com"
SITE_NAME = "PeriodicaldataextractionsOIE-WAHIS"
FOLDER_PATH = "Shared Documents"  # relative to the site

METADATA_FILENAME = "Metadata_WeeklyExtraction.xlsx"
INFUR_PATTERN = re.compile(r"^infur_(.+)\.xlsx$", re.IGNORECASE)

DEFAULT_DOWNLOAD_DIR = Path("./downloads")

# ── SharePoint REST helpers ───────────────────────────────────────────────────

def _sp_base(site_url: str) -> str:
    return f"{site_url}/_api/web"


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;odata=verbose",
    }


def get_site_url(token: str) -> str:
    """Resolve the full site URL using SharePoint REST."""
    url = (
        f"https://{SHAREPOINT_HOST}/sites/{SITE_NAME}"
        f"/_api/web?$select=Url"
    )
    r = requests.get(url, headers=_headers(token), timeout=30)
    r.raise_for_status()
    return r.json()["d"]["Url"]


def list_files(token: str, site_url: str, folder: str) -> list[dict]:
    """
    Returns list of dicts with keys: Name, TimeLastModified, ServerRelativeUrl, Length
    """
    encoded_folder = folder.replace(" ", "%20")
    url = (
        f"{_sp_base(site_url)}"
        f"/GetFolderByServerRelativeUrl('{encoded_folder}')"
        f"/Files?$select=Name,TimeLastModified,ServerRelativeUrl,Length"
        f"&$orderby=TimeLastModified%20desc"
    )
    r = requests.get(url, headers=_headers(token), timeout=30)
    if r.status_code == 403:
        raise PermissionError(
            "Access denied to SharePoint folder. "
            "The device code flow may not be permitted by UCSD IT policy. "
            "See IT request template in it_request.md."
        )
    r.raise_for_status()
    return r.json()["d"]["results"]


def find_latest_infur(files: list[dict]) -> dict | None:
    """Find the most recently modified infur_*.xlsx file."""
    infur_files = [f for f in files if INFUR_PATTERN.match(f["Name"])]
    if not infur_files:
        return None
    # Already sorted by TimeLastModified desc from the API
    return infur_files[0]


def download_file(token: str, site_url: str, server_relative_url: str, dest: Path) -> Path:
    """Stream-download a file by its server-relative URL."""
    download_url = f"{site_url}/_layouts/15/download.aspx?SourceUrl={server_relative_url}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "*/*",
    }
    dest.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(download_url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)

    size_kb = dest.stat().st_size / 1024
    print(f"  ✓ {dest.name}  ({size_kb:.1f} KB)  →  {dest}")
    return dest


# ── Main logic ────────────────────────────────────────────────────────────────

def run(
    download_dir: Path,
    force_reauth: bool = False,
    list_only: bool = False,
    also_metadata: bool = False,
) -> list[Path]:
    """
    Core download routine. Returns list of downloaded file paths.
    Safe to call from Dagster ops.
    """
    print("Authenticating...")
    token = get_token(force_reauth=force_reauth)

    print("Connecting to SharePoint...")
    site_url = get_site_url(token)
    folder_server_path = f"/sites/{SITE_NAME}/{FOLDER_PATH}"

    print(f"Listing files in: {folder_server_path}")
    files = list_files(token, site_url, folder_server_path)
    print(f"  Found {len(files)} file(s) in folder.")

    if list_only:
        print("\nAll files:")
        for f in files:
            size_kb = int(f.get("Length", 0)) / 1024
            print(f"  {f['Name']:<50} {size_kb:>8.1f} KB   {f['TimeLastModified']}")
        return []

    downloaded = []

    # ── Download latest infur file ──────────────────────────────────────────
    latest = find_latest_infur(files)
    if not latest:
        print("ERROR: No infur_*.xlsx files found in the SharePoint folder.")
        sys.exit(1)

    print(f"\nLatest infur file: {latest['Name']}  (modified {latest['TimeLastModified']})")
    dest = download_dir / latest["Name"]

    if dest.exists():
        print(f"  Already downloaded: {dest} — skipping.")
    else:
        downloaded.append(
            download_file(token, site_url, latest["ServerRelativeUrl"], dest)
        )

    # ── Optionally download metadata ────────────────────────────────────────
    if also_metadata:
        meta_file = next((f for f in files if f["Name"] == METADATA_FILENAME), None)
        if meta_file:
            meta_dest = download_dir / METADATA_FILENAME
            downloaded.append(
                download_file(token, site_url, meta_file["ServerRelativeUrl"], meta_dest)
            )
        else:
            print(f"  WARNING: {METADATA_FILENAME} not found in folder.")

    return downloaded


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Download OIE-WAHIS infur data from SharePoint (unattended after first auth)."
    )
    parser.add_argument("--download-dir", type=Path, default=DEFAULT_DOWNLOAD_DIR)
    parser.add_argument("--reauth", action="store_true", help="Force re-authentication")
    parser.add_argument("--list-only", action="store_true", help="List files, don't download")
    parser.add_argument("--metadata", action="store_true", help="Also download metadata file")
    args = parser.parse_args()

    print("=" * 65)
    print("  OIE-WAHIS SharePoint Downloader")
    print(f"  Site:    https://{SHAREPOINT_HOST}/sites/{SITE_NAME}")
    print(f"  Output:  {args.download_dir.resolve()}")
    print("=" * 65)

    paths = run(
        download_dir=args.download_dir,
        force_reauth=args.reauth,
        list_only=args.list_only,
        also_metadata=args.metadata,
    )

    if paths:
        print(f"\n✓ Done. {len(paths)} file(s) downloaded.")


if __name__ == "__main__":
    main()
