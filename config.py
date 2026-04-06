import os
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent
CORE_REPOS_DIR = PIPELINE_DIR.parent
CHRIS_DIR = CORE_REPOS_DIR / "Chris"

# Prefer credentials and geo data shipped next to this package (no env required).
LOCAL_SERVICE_ACCOUNT_JSON = PIPELINE_DIR / "zoomcasa-scaler-key1-5b442b14e7cd.json"
LOCAL_GEOREF_JSON = PIPELINE_DIR / "georef-united-states-of-america-zc-point.json"

NOTEBOOK_SERVICE_ACCOUNT_JSON = (
    CHRIS_DIR / "zoomcasa-scaler-key1-5b442b14e7cd.json"
)

NOTEBOOK_SPREADSHEET_ID = "1Dr5RvMOMgTcp94S-Q1X4PPAzkrq-HrrryLuTvyNoYWI"

NOTEBOOK_GEOREF_JSON = CHRIS_DIR / "georef-united-states-of-america-zc-point.json"

LEGACY_ABSOLUTE_JSON = Path(
    "/Users/apple/Downloads/Core Repos/Chris/zoomcasa-scaler-key1-5b442b14e7cd.json"
)


def resolve_gsheet_credentials_path() -> str:
    v = os.environ.get("GSHEET_SERVICE_ACCOUNT_JSON")
    if v and str(v).strip():
        return str(v).strip()
    if LOCAL_SERVICE_ACCOUNT_JSON.is_file():
        return str(LOCAL_SERVICE_ACCOUNT_JSON)
    if NOTEBOOK_SERVICE_ACCOUNT_JSON.is_file():
        return str(NOTEBOOK_SERVICE_ACCOUNT_JSON)
    if LEGACY_ABSOLUTE_JSON.is_file():
        return str(LEGACY_ABSOLUTE_JSON)
    raise RuntimeError(
        "Set GSHEET_SERVICE_ACCOUNT_JSON to the path of your Google service account JSON, or place "
        "zoomcasa-scaler-key1-5b442b14e7cd.json in this project directory or under ../Chris "
        f"(expected: {LOCAL_SERVICE_ACCOUNT_JSON} or {NOTEBOOK_SERVICE_ACCOUNT_JSON})."
    )


def resolve_gsheet_spreadsheet_id() -> str:
    v = os.environ.get("GSHEET_SPREADSHEET_ID")
    if v and str(v).strip():
        return str(v).strip()
    return NOTEBOOK_SPREADSHEET_ID


def resolve_georef_json() -> str:
    v = os.environ.get("GEOREF_JSON")
    if v and str(v).strip():
        return str(v).strip()
    if LOCAL_GEOREF_JSON.is_file():
        return str(LOCAL_GEOREF_JSON)
    if NOTEBOOK_GEOREF_JSON.is_file():
        return str(NOTEBOOK_GEOREF_JSON)
    return "georef-united-states-of-america-zc-point.json"
