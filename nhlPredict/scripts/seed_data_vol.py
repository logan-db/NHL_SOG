#!/usr/bin/env python3
"""
Upload source CSV files into the target's UC data volume via the REST Files API.

IMPORTANT — why this script uses curl/REST rather than the Databricks CLI or SDK:
  - `databricks fs cp "dbfs:/Volumes/..."` → writes to LEGACY DBFS, not UC volumes.
    Spark reads /Volumes/... from UC volumes; the two storage layers are separate.
  - `w.files.upload()` in older SDK versions also routes through DBFS.
  - The REST Files API endpoint /api/2.0/fs/files IS the UC volumes write path.
    HTTP 204 response = file confirmed in UC volumes, readable by spark.read.csv().

Volume paths (verified working via SQL LIST + read_files):
  dev:  /Volumes/lr_nhl_demo/dev/logan_rupert/data/team_code_mappings.csv
  prod: /Volumes/lr_nhl_demo/prod/data/team_code_mappings.csv

Usage:
  DATABRICKS_CONFIG_PROFILE=DEFAULT python scripts/seed_data_vol.py prod
  DATABRICKS_CONFIG_PROFILE=DEFAULT python scripts/seed_data_vol.py dev
"""

import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path


HOST = "https://e2-demo-field-eng.cloud.databricks.com"

VOLUME_PATHS = {
    "dev":  "/Volumes/lr_nhl_demo/dev/logan_rupert/data",
    "prod": "/Volumes/lr_nhl_demo/prod/data",
}

FILES = ["team_code_mappings.csv"]


def get_token(profile: str) -> str:
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)["access_token"]


def upload(token: str, local_path: Path, uc_path: str) -> None:
    url = f"{HOST}/api/2.0/fs/files{uc_path}?overwrite=true"
    data = local_path.read_bytes()
    req = urllib.request.Request(url, data=data, method="PUT")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/octet-stream")
    try:
        resp = urllib.request.urlopen(req)
        print(f"  HTTP {resp.status} ✓  {local_path.name}  ({len(data):,} bytes) → {uc_path}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  HTTP {e.code} ✗  {local_path.name}: {body}")
        raise


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else ""
    if target not in VOLUME_PATHS:
        print(f"Usage: python scripts/seed_data_vol.py <target>")
        print(f"  targets: {list(VOLUME_PATHS.keys())}")
        sys.exit(1)

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    data_dir = Path(__file__).parent.parent / "src" / "data"
    dest_base = VOLUME_PATHS[target]

    print(f"Uploading to {dest_base}  (target={target}, profile={profile})")
    token = get_token(profile)

    for fname in FILES:
        src = data_dir / fname
        if not src.exists():
            print(f"  SKIP — not found locally: {fname}")
            continue
        upload(token, src, f"{dest_base}/{fname}")

    print("\nDone. Verify with SQL:")
    print(f"  SELECT * FROM read_files('{dest_base}/team_code_mappings.csv', format=>'csv', header=>'true') LIMIT 3")


if __name__ == "__main__":
    main()
