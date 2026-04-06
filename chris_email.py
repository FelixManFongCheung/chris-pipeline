import os
import re
import time
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd

from config import resolve_gsheet_credentials_path, resolve_gsheet_spreadsheet_id
import sib_api_v3_sdk
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from sib_api_v3_sdk.rest import ApiException
from tqdm import tqdm
import gspread

load_dotenv(Path(__file__).resolve().parent / ".env")


def _creds_path():
    return resolve_gsheet_credentials_path()


def _sheet_id():
    return resolve_gsheet_spreadsheet_id()


def _brevo_client():
    key = os.environ.get("BREVO_API_KEY")
    if not key:
        raise RuntimeError("Set BREVO_API_KEY")
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key["api-key"] = key
    return sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))


def _gspread_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(_creds_path(), scope)
    return gspread.authorize(creds)


def load_contacted_dataframe(ws_contacted):
    values = ws_contacted.get_all_values()
    if not values or len(values) < 2:
        print("'contacted' sheet is empty or has no data rows")
        return pd.DataFrame()
    headers = values[0]
    seen = {}
    clean_headers = []
    for i, h in enumerate(headers):
        h = (h or "").strip()
        if not h:
            h = f"__col_{i+1}"
        if h in seen:
            seen[h] += 1
            h = f"{h}__{seen[h]}"
        else:
            seen[h] = 0
        clean_headers.append(h)
    df = pd.DataFrame(values[1:], columns=clean_headers)
    df = df.replace("", pd.NA).dropna(how="all").reset_index(drop=True)
    if "estimated_equity_percentage" in df.columns:
        df["estimated_equity_percentage"] = (
            pd.to_numeric(df["estimated_equity_percentage"], errors="coerce").astype("Int64")
        )
    return df


def is_valid_email(email):
    if pd.isna(email) or email == "" or str(email).lower() == "nan":
        return False
    email = str(email).strip()
    if ";" in email or "," in email or " " in email:
        return False
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


def build_email_previews_from_log(df_log: pd.DataFrame, all_previously_handled_mls=None):
    if all_previously_handled_mls is None:
        all_previously_handled_mls = set()
    req = ["Email", "Phone", "Name_mls", "Address", "Listing Price", "MLS #"]
    missing = [c for c in req if c not in df_log.columns]
    if missing:
        raise ValueError(
            f"df_log missing required columns: {missing}\nAvailable columns: {list(df_log.columns)}"
        )
    df = df_log.copy()
    df["_original_index"] = df.index
    df["MLS #"] = df["MLS #"].astype(str).str.strip()
    df["Listing Price"] = (
        df["Listing Price"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace("", pd.NA)
        .astype(float)
    )
    if "estimated_equity_percentage" in df.columns:
        df["estimated_equity_percentage"] = (
            pd.to_numeric(df["estimated_equity_percentage"], errors="coerce").astype("Int64")
        )
    else:
        df["estimated_equity_percentage"] = pd.Series([pd.NA] * len(df), dtype="Int64")
    df["Email"] = df["Email"].astype(str).str.strip()
    df = df[
        (df["Email"].notna())
        & (df["Email"] != "")
        & (df["Email"].str.lower() != "nan")
    ]
    email_previews_plain = []
    offer_counts = {"offer_a": 0, "offer_b": 0, "offer_c": 0}
    processed_emails = {}
    email_to_first_row = {}
    for idx, row in df.iterrows():
        email = row["Email"]
        if email not in email_to_first_row:
            email_to_first_row[email] = idx
    for idx, row in df.iterrows():
        email = row["Email"]
        if email in processed_emails:
            continue
        processed_emails[email] = True
        g = df[df["Email"] == email].sort_values(
            "Listing Price", ascending=False, na_position="last"
        ).reset_index(drop=True)
        listings = g.to_dict(orient="records")
        if not listings:
            continue
        primary = listings[0]
        full_name = str(primary.get("Name_mls", "")).strip()
        first_name = (
            full_name.split(",")[0].strip()
            if "," in full_name
            else full_name.split(" ")[0]
        )
        address = str(primary.get("Address", "")).strip()
        price = primary.get("Listing Price")
        price_num = float(price) if pd.notna(price) else 0.0
        mls = str(primary.get("MLS #", "")).strip()
        phone = primary.get("Phone", "")
        est_equity = primary.get("estimated_equity_percentage", pd.NA)
        other_addresses = [
            str(r.get("Address", "")).strip()
            for r in listings[1:]
            if str(r.get("Address", "")).strip()
        ]
        address_suffix = " & " + ", ".join(other_addresses) if other_addresses else ""
        subject = f"Fast Equity Solution for Your Listing {address}{address_suffix and f' ({address_suffix})'}"
        intro = f"""Hi {first_name},

My team at Zoom Casa has been reviewing your listing at {address} (MLS #{mls}), and we believe it's a strong fit for our equity-forward structure.

There this should become:

We specialize in helping sellers unlock liquidity quickly without the friction of showings or prep work. This is not a wholesale model. Our program is a two-step purchase where sellers typically retain approx. 90-95% of the property's value, including any upside if we sell above 100% of that value.

We advance up to 75% of the as-is appraised value within about two weeks of signing, then the remainder after resale. The structure gives your client fast access to equity to pay off mortgages, move, or buy their next home while avoiding the delays and uncertainty of a traditional sale. Of course, you stay on as the listing agent for the resale.

We'd like to make a purchase offer.
"""
        offer_template = f"""
1. BUYER: Zoom Casa, LLC

2. PURCHASE PRICE: ${price_num:,.0f}

3. FINANCING TERMS: All Cash – no financing contingency

4. CLOSING DATE: 15 days after acceptance (or longer should seller desire more time)

5. CONTINGENCIES: 10 day inspection and appraisal contingency

6. OTHER TERMS: Seller to receive up to {{equity_rate}} of appraised as-is value initially plus
    additional proceeds upon Buyer's resale of the property, as applicable

7. TITLE / ESCROW: Seller's choice

8. CLOSING COSTS: Buyer and seller to split escrow costs.
    Seller to pay transfer taxes and buyer's title policy.

9. EQUITY GUIDANCE: This structure typically works best for properties with at least 30–40 percent equity which supports a smooth closing and optimal seller proceeds.
"""
        if price_num < 1_000_000:
            equity_rate = "75%"
            offer_counts["offer_a"] += 1
        elif price_num < 1_500_000:
            equity_rate = "70%"
            offer_counts["offer_b"] += 1
        else:
            equity_rate = "70%"
            offer_counts["offer_c"] += 1
        offer_body = offer_template.replace("{equity_rate}", equity_rate)
        closing = """
I've already briefed our team's top Account Executive, on your listing. If this approach could be helpful, I can have him give you a quick call to explore further.
"""
        text_signature = """
Kind regards,

Christian Garcia, UCLA PhD
Sales & Market Intelligence
Zoom Casa

Even if this particular property doesn't move forward, it's great to connect you with Zoom Casa. This type of solution often ends up being the perfect fit for sellers but isn't always known as an option.
"""
        ps_line = ""
        if len(listings) > 1:
            listing_refs = [
                f"{r['Address']} at ${float(pd.to_numeric(r['Listing Price'], errors='coerce')):,.0f}"
                for r in listings[1:]
                if r.get("Address")
            ]
            if listing_refs:
                ps_line = (
                    "P.S. The same offer structure applies for your other listings: "
                    + ", ".join(listing_refs)
                    + "."
                )
        full_message = f"{intro}\n{offer_body}\n{closing}\n{text_signature}\n\n{ps_line}"
        phone_clean = (
            str(phone)
            .replace("-", "")
            .replace("(", "")
            .replace(")", "")
            .replace(" ", "")
            .strip()
        )
        email_previews_plain.append(
            {
                "Email": email,
                "Phone": phone_clean,
                "First Name": first_name,
                "Subject": subject,
                "Plain_Text_Message": full_message,
                "Estimated_Equity": est_equity,
                "Listing Price": price_num,
                "_original_sheet_order": idx,
            }
        )
    email_previews_df = pd.DataFrame(email_previews_plain)
    if "_original_sheet_order" in email_previews_df.columns:
        email_previews_df = email_previews_df.sort_values(
            "_original_sheet_order"
        ).reset_index(drop=True)
        email_previews_df = email_previews_df.drop(columns=["_original_sheet_order"])
    return email_previews_df


def main():
    batch_start = int(os.environ.get("BATCH_START", "0"))
    batch_size = int(os.environ.get("BATCH_SIZE", "299"))
    sender_email = os.environ.get("SENDER_EMAIL", "christian@zoomcasa.com")
    rate_sleep = float(os.environ.get("EMAIL_RATE_SLEEP", "0.4"))

    client = _gspread_client()
    sh_main = client.open_by_key(_sheet_id())
    ws_contacted = sh_main.worksheet("contacted")

    df_contacted = load_contacted_dataframe(ws_contacted)
    print(f"Loaded {len(df_contacted)} rows from 'contacted' sheet")
    if len(df_contacted) > 0 and "Email" in df_contacted.columns:
        print(df_contacted["Email"].head(5).tolist())

    if len(df_contacted) == 0:
        print("Cannot build email previews — 'contacted' sheet is empty")
        return

    email_previews_df = build_email_previews_from_log(
        df_contacted, all_previously_handled_mls=set()
    )
    print(f"Built {len(email_previews_df)} email previews")
    if len(email_previews_df) > 0:
        print(email_previews_df["Email"].head(5).tolist())

    brevo_client = _brevo_client()
    preview = email_previews_df.copy()
    preview["Email"] = preview["Email"].astype(str).str.strip()
    preview = preview[
        (preview["Email"] != "") & (preview["Email"].str.lower() != "nan")
    ].reset_index(drop=True)
    batch = preview.iloc[batch_start : batch_start + batch_size]

    print(
        f"Sending {len(batch)} emails (rows {batch_start}–{batch_start + len(batch) - 1})"
    )

    sent_emails = set()
    invalid_emails = set()
    failed_emails = []

    for _, row in tqdm(batch.iterrows(), total=len(batch)):
        email = row["Email"]
        subject = row["Subject"]
        message_text = row["Plain_Text_Message"]
        if not is_valid_email(email):
            print(f"Skipping invalid email format: {email}")
            invalid_emails.add(email)
            continue
        try:
            email_payload = sib_api_v3_sdk.SendSmtpEmail(
                to=[{"email": email}],
                sender={"email": sender_email},
                subject=subject,
                text_content=message_text,
            )
            brevo_client.send_transac_email(email_payload)
            sent_emails.add(email)
            time.sleep(rate_sleep)
        except ApiException as e:
            if e.status == 400 and "invalid" in str(e.body).lower():
                print(f"Invalid email format: {email}")
                invalid_emails.add(email)
            else:
                print(f"Failed email to {email}: {e}")
                failed_emails.append(email)

    print(f"Successfully sent: {len(sent_emails)} emails")
    print(f"Invalid emails (removed): {len(invalid_emails)} emails")
    print(f"Failed (other errors): {len(failed_emails)} emails")

    emails_to_remove = sent_emails | invalid_emails
    if emails_to_remove:
        df_contacted_updated = load_contacted_dataframe(ws_contacted)
        if "Email" in df_contacted_updated.columns:
            df_contacted_updated["Email"] = df_contacted_updated["Email"].astype(
                str
            ).str.strip()
            df_contacted_remaining = df_contacted_updated[
                ~df_contacted_updated["Email"].isin(emails_to_remove)
            ].reset_index(drop=True)
            contacted_values = ws_contacted.get_all_values()
            if contacted_values:
                headers = contacted_values[0]
                ws_contacted.clear()
                ws_contacted.update(range_name="A1", values=[headers])
                if len(df_contacted_remaining) > 0:
                    set_with_dataframe(
                        ws_contacted,
                        df_contacted_remaining,
                        row=2,
                        include_column_header=False,
                    )
                print(
                    f"Removed {len(sent_emails)} sent + {len(invalid_emails)} invalid from 'contacted' sheet; remaining: {len(df_contacted_remaining)}"
                )
            else:
                print("Could not update 'contacted' sheet — no headers found")
        else:
            print("'Email' column not found in contacted sheet")
    else:
        print("No emails sent or invalid; 'contacted' sheet unchanged")


if __name__ == "__main__":
    main()
