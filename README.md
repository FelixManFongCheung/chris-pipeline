# chris-pipeline

Python ports of `AZ/Sender4Felix.ipynb` (`chris_email.py`) and `AZ/homeharvest_scrap.ipynb` (`scrape.py`).

## Google credentials (same as the notebook)

`Sender4Felix` uses:

- `ppath = ".../Chris/zoomcasa-scaler-key1-5b442b14e7cd.json"`

This project resolves credentials in order:

1. `GSHEET_SERVICE_ACCOUNT_JSON` from the environment (or `.env` loaded next to these scripts)
2. `../Chris/zoomcasa-scaler-key1-5b442b14e7cd.json` relative to the `chris-pipeline` folder
3. The legacy absolute path used in the notebook (if that file exists on disk)

Spreadsheet id defaults to the same id as in the notebook if `GSHEET_SPREADSHEET_ID` is unset.

Copy `.env.example` to `.env` to override paths and API keys. `.env` is gitignored.
