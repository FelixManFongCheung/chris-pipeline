"""
Run pipeline based on 'contacted' valid-email count.

- If count <= CONTACTED_EMAIL_THRESHOLD (default 299): run scrape.py, then
  chris_email.py (only if scrape.py succeeds).
- If count > threshold: run chris_email.py only.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv


def count_valid_emails_in_contacted() -> int:
    from chris_email import _gspread_client, is_valid_email, load_contacted_dataframe
    from config import resolve_gsheet_spreadsheet_id

    client = _gspread_client()
    sh = client.open_by_key(resolve_gsheet_spreadsheet_id())
    ws = sh.worksheet("contacted")
    df = load_contacted_dataframe(ws)
    if df.empty or "Email" not in df.columns:
        return 0
    return int(df["Email"].map(lambda e: is_valid_email(e)).sum())


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")

    raw = os.environ.get("CONTACTED_EMAIL_THRESHOLD", "299").strip()
    try:
        threshold = int(raw)
    except ValueError as e:
        raise SystemExit(
            f"CONTACTED_EMAIL_THRESHOLD must be an integer, got {raw!r}"
        ) from e

    n = count_valid_emails_in_contacted()
    print(
        f"Orchestrator: 'contacted' has {n} valid email(s); "
        f"threshold is {threshold} "
        "(<= threshold → scrape.py then chris_email.py, else → chris_email.py)."
    )

    root = Path(__file__).resolve().parent
    if n <= threshold:
        print("→ Running scrape.py (full pipeline from SCRAPE_STEPS / .env)")
        scrape_proc = subprocess.run([sys.executable, str(root / "scrape.py")], cwd=root)
        if scrape_proc.returncode != 0:
            print(f"✗ scrape.py failed with exit code {scrape_proc.returncode}; skipping chris_email.py")
            raise SystemExit(scrape_proc.returncode)

        print("→ scrape.py succeeded; running chris_email.py")
        proc = subprocess.run([sys.executable, str(root / "chris_email.py")], cwd=root)
    else:
        print("→ Running chris_email.py")
        proc = subprocess.run([sys.executable, str(root / "chris_email.py")], cwd=root)

    raise SystemExit(proc.returncode)


if __name__ == "__main__":
    main()
