import json
import os
import sys
import ast
import re
import time
import requests
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from difflib import SequenceMatcher

from dotenv import load_dotenv
import pandas as pd

from config import (
    resolve_georef_json,
    resolve_gsheet_credentials_path,
    resolve_gsheet_spreadsheet_id,
)
from fuzzywuzzy import fuzz
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
import gspread
from gspread_dataframe import set_with_dataframe
from homeharvest import scrape_property

load_dotenv(Path(__file__).resolve().parent / ".env")


def clean_zip_code(address: str) -> str:
    if not address:
        return address
    pattern = '(\\d{5})-\\d{4}'
    cleaned = re.sub(pattern, '\\1', address)
    return cleaned

def generate_address_variations(address: str) -> List[str]:
    variations = []
    cleaned = clean_zip_code(address)
    variations.append(cleaned)
    parts = [p.strip() for p in cleaned.split(',')]
    if len(parts) > 1:
        variations.append(','.join(parts[:-1]))
        if len(parts) > 2:
            variations.append(','.join(parts[:-2]))
        if len(parts) >= 2:
            variations.append(f'{parts[0]}, {parts[1]}')
        variations.append(parts[0])
    seen = set()
    unique_variations = []
    for v in variations:
        if v and v not in seen:
            seen.add(v)
            unique_variations.append(v)
    return unique_variations

def normalize_address(address: str) -> str:
    if not address:
        return ''
    return ' '.join(address.lower().strip().split())

def address_similarity(addr1: str, addr2: str) -> float:
    if not addr1 or not addr2:
        return 0.0
    return SequenceMatcher(None, normalize_address(addr1), normalize_address(addr2)).ratio()

def find_matching_record(records: list, target_address: str, similarity_threshold: float=0.85) -> Optional[Dict[Any, Any]]:
    if not records or not target_address:
        return None
    best_match = None
    best_score = 0.0
    target_normalized = normalize_address(target_address)
    for record in records:
        record_address = None
        addr_field = record.get('address')
        if isinstance(addr_field, dict):
            record_address = addr_field.get('address') or addr_field.get('street')
        elif isinstance(addr_field, str):
            record_address = addr_field
        if not record_address:
            record_address = record.get('street') or record.get('title')
        if not record_address:
            continue
        score = address_similarity(target_address, record_address)
        if normalize_address(target_address) == normalize_address(record_address):
            return record
        if score > best_score:
            best_score = score
            best_match = record
    if best_match and best_score >= similarity_threshold:
        return best_match
    return None

def call_primetracers_property_search(address: str, client_uuid: str=None, verbose: bool=True, max_retries: int=3) -> Optional[Dict[Any, Any]]:
    url = 'https://app.primetracers.com/api/rei/property-search'
    # Do not advertise br/zstd: the API often responds with Content-Encoding: br, and requests/urllib3
    # only decompresses Brotli if the optional brotli package is installed; otherwise response.json() fails.
    headers = {'accept': '*/*', 'accept-encoding': 'gzip, deflate', 'accept-language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7', 'content-type': 'application/json', 'dnt': '1', 'origin': 'https://app.primetracers.com', 'priority': 'u=1, i', 'referer': 'https://app.primetracers.com/explore', 'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"', 'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"macOS"', 'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-origin', 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'}
    payload = {'data': {'address': address, 'searchTerm': address, 'searchType': 'A', 'size': 100, 'resultIndex': 0, 'summary': True}}
    cookies = {}
    if client_uuid:
        cookies['client_uuid'] = client_uuid
    for attempt in range(max_retries):
        try:
            if verbose and attempt > 0:
                print(f'  🔄 Retry attempt {attempt + 1}/{max_retries}')
            response = requests.post(url, headers=headers, json=payload, cookies=cookies if cookies else None, timeout=30)
            if verbose:
                print(f'  📊 Status: {response.status_code}')
            if response.status_code == 200:
                try:
                    data = response.json()
                    if verbose:
                        print(f'  ✅ Success')
                    return data
                except json.JSONDecodeError as e:
                    if verbose:
                        print(f'  ❌ Failed to parse JSON response: {e}')
                        print(
                            f'  📎 content-encoding={response.headers.get("content-encoding")!r} '
                            f'bytes={len(response.content)}'
                        )
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None
            elif response.status_code == 429:
                wait_time = 2 ** attempt * 5
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                    except ValueError:
                        pass
                if attempt < max_retries - 1:
                    print(f'  ⏱️ Rate limited. Waiting {wait_time}s before retry...')
                    time.sleep(wait_time)
                    continue
                else:
                    print(f'  ❌ Rate limited after {max_retries} attempts')
                    return None
            elif response.status_code == 403:
                print(f'  ❌ Forbidden - IP may be blocked (stopping)')
                return None
            elif response.status_code == 404:
                print(f'  ❌ Not found - endpoint may have changed')
                return None
            elif attempt < max_retries - 1:
                wait_time = 2 ** attempt * 2
                print(f'  ⚠️ Error {response.status_code}. Retrying in {wait_time}s...')
                time.sleep(wait_time)
                continue
            else:
                print(f'  ❌ Error {response.status_code}: {response.text[:200]}')
                return None
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt * 2
                print(f'  ⏱️ Timeout. Retrying in {wait_time}s...')
                time.sleep(wait_time)
                continue
            else:
                print(f'  ❌ Timeout after {max_retries} attempts')
                return None
        except requests.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt * 2
                print(f'  ⚠️ Connection error: {e}. Retrying in {wait_time}s...')
                time.sleep(wait_time)
                continue
            else:
                print(f'  ❌ Connection failed after {max_retries} attempts: {e}')
                return None
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt * 2
                print(f'  ⚠️ Request failed: {e}. Retrying in {wait_time}s...')
                time.sleep(wait_time)
                continue
            else:
                if verbose:
                    print(f'  ❌ Request failed after {max_retries} attempts: {e}')
                return None
    return None

def extract_property_data_from_response(response_data: Dict[Any, Any], target_address: str, verbose: bool=True) -> Optional[Dict[str, Any]]:
    try:
        if not isinstance(response_data, dict):
            if verbose:
                print(f'  ⚠️ Response is not a dictionary')
            return None
        records = response_data.get('records', [])
        if not records or len(records) == 0:
            if verbose:
                print(f'  ⚠️ No records found in response')
            return None
        if verbose:
            print(f'  📋 Found {len(records)} record(s) in response')
        matching_record = find_matching_record(records, target_address)
        if matching_record:
            matched_addr = None
            addr_field = matching_record.get('address')
            if isinstance(addr_field, dict):
                matched_addr = addr_field.get('address')
            elif isinstance(addr_field, str):
                matched_addr = addr_field
            if verbose and matched_addr:
                print(f'  ✅ Matched record: {matched_addr}')
            extracted_data = {}

            def safe_get(key, default=None):
                value = matching_record.get(key, default)
                return value if value is not None else default
            extracted_data['estimatedMortgagePayment'] = safe_get('estimatedMortgagePayment')
            extracted_data['negativeEquity'] = safe_get('negativeEquity', False)
            extracted_data['inStateAbsenteeOwner'] = safe_get('inStateAbsenteeOwner', False)
            extracted_data['absenteeOwner'] = safe_get('absenteeOwner', False)
            extracted_data['outOfStateAbsenteeOwner'] = safe_get('outOfStateAbsenteeOwner', False)
            extracted_data['ownerOccupied'] = safe_get('ownerOccupied', False)
            extracted_data['mlsDaysOnMarket'] = safe_get('mlsDaysOnMarket')
            extracted_data['yearsOwned'] = safe_get('yearsOwned')
            extracted_data['propertyType'] = safe_get('propertyType')
            extracted_data['medianIncome'] = safe_get('medianIncome')
            extracted_data['inherited'] = safe_get('inherited', False)
            extracted_data['death'] = safe_get('death', False)
            extracted_data['vacant'] = safe_get('vacant', False)
            extracted_data['corporateOwned'] = safe_get('corporateOwned', False)
            extracted_data['investorBuyer'] = safe_get('investorBuyer', False)
            extracted_data['taxLien'] = safe_get('taxLien', False)
            extracted_data['judgment'] = safe_get('judgment', False)
            extracted_data['preForeclosure'] = safe_get('preForeclosure', False)
            extracted_data['foreclosure'] = safe_get('foreclosure', False)
            extracted_data['auction'] = safe_get('auction', False)
            extracted_data['reo'] = safe_get('reo', False)
            extracted_data['equityPercent'] = safe_get('equityPercent')
            if verbose:
                print(f'  ✅ Extracted {len([k for k, v in extracted_data.items() if v is not None])} field(s)')
            return extracted_data
        else:
            if verbose:
                print(f'  ⚠️ No matching record found for address: {target_address}')
                print(f'  📋 Available addresses in response:')
                for i, record in enumerate(records[:3]):
                    addr_field = record.get('address')
                    if isinstance(addr_field, dict):
                        addr = addr_field.get('address', 'N/A')
                    elif isinstance(addr_field, str):
                        addr = addr_field
                    else:
                        addr = 'N/A'
                    print(f'      {i + 1}. {addr}')
            return None
    except KeyError as e:
        if verbose:
            print(f'  ⚠️ KeyError extracting property data: {e}')
        return None
    except (ValueError, TypeError, IndexError) as e:
        if verbose:
            print(f'  ⚠️ Error extracting property data: {e}')
        return None
    except Exception as e:
        if verbose:
            print(f'  ⚠️ Unexpected error extracting property data: {type(e).__name__}: {e}')
        return None

def extract_equity_percent_from_response(response_data: Dict[Any, Any], target_address: str, verbose: bool=True) -> Optional[float]:
    property_data = extract_property_data_from_response(response_data, target_address, verbose)
    if property_data:
        equity_percent = property_data.get('equityPercent')
        if equity_percent is not None:
            try:
                return float(equity_percent)
            except (ValueError, TypeError):
                return None
    return None

def try_address_variations(original_address: str, client_uuid: str=None, verbose: bool=True) -> tuple[Optional[Dict[Any, Any]], Optional[str]]:
    variations = generate_address_variations(original_address)
    if verbose:
        print(f'  🔍 Trying {len(variations)} address variation(s)')
    for i, addr_variant in enumerate(variations):
        if verbose and i > 0:
            print(f"  🔄 Fallback {i}: Trying '{addr_variant}'")
        response_data = call_primetracers_property_search(address=addr_variant, client_uuid=client_uuid, verbose=verbose and i == 0, max_retries=1)
        if response_data:
            records = response_data.get('records', [])
            if records and len(records) > 0:
                matching_record = find_matching_record(records, addr_variant)
                if matching_record and matching_record.get('equityPercent') is not None:
                    if verbose and i > 0:
                        print(f"  ✅ Found with fallback address: '{addr_variant}'")
                    return (response_data, addr_variant)
        if i < len(variations) - 1:
            time.sleep(0.5)
    return (None, None)

def enrich_sampled_merged_with_primetracers(sampled_merged: pd.DataFrame, client_uuid: str=None, delay_between: float=2.5, use_fallback: bool=True, start_idx: int=0, verbose: bool=True) -> pd.DataFrame:
    print(len(sampled_merged))
    out = sampled_merged.copy().reset_index(drop=True)
    new_columns = ['estimatedMortgagePayment', 'negativeEquity', 'inStateAbsenteeOwner', 'absenteeOwner', 'outOfStateAbsenteeOwner', 'ownerOccupied', 'mlsDaysOnMarket', 'yearsOwned', 'propertyType', 'medianIncome', 'inherited', 'death', 'vacant', 'corporateOwned', 'investorBuyer', 'taxLien', 'judgment', 'preForeclosure', 'foreclosure', 'auction', 'reo']
    for col in new_columns:
        if col not in out.columns:
            out[col] = None
    if 'estimated_equity_percentage' not in out.columns:
        out['estimated_equity_percentage'] = None
    total = len(out)
    consecutive_errors = 0
    max_consecutive_errors = 5
    for idx in range(start_idx, total):
        row = out.iloc[idx]
        original_address = str(row.get('Address', '')).strip()
        cleaned_address = clean_zip_code(original_address)
        if verbose and cleaned_address != original_address:
            print(f"  🧹 Cleaned ZIP: '{original_address}' -> '{cleaned_address}'")
        if not cleaned_address or cleaned_address.lower() in ('none', 'n/a', 'nan', ''):
            if verbose:
                print(f"\n{'=' * 70}\n⏭️  Skipping row {idx + 1}/{total}: Empty address\n{'=' * 70}")
            continue
        primary_address = ','.join(cleaned_address.split(',')[:-1]) if ',' in cleaned_address else cleaned_address
        print(f"\n{'=' * 70}\n🏠 Processing {idx + 1}/{total}: {primary_address}\n{'=' * 70}")
        try:
            if use_fallback:
                response_data, matched_address = try_address_variations(original_address=cleaned_address, client_uuid=client_uuid, verbose=verbose)
            else:
                response_data = call_primetracers_property_search(address=primary_address, client_uuid=client_uuid, verbose=verbose, max_retries=3)
                matched_address = primary_address if response_data else None
            if response_data:
                target_for_matching = matched_address if matched_address else primary_address
                property_data = extract_property_data_from_response(response_data, target_address=target_for_matching, verbose=verbose)
                if property_data is None:
                    print(f'⚠️  No matching address found - all fields set to blank')
                    consecutive_errors += 1
                else:
                    for field_name, field_value in property_data.items():
                        if field_name == 'equityPercent':
                            out.at[idx, 'estimated_equity_percentage'] = field_value
                        elif field_name in out.columns:
                            out.at[idx, field_name] = field_value
                    equity_percent = property_data.get('equityPercent')
                    if equity_percent is not None:
                        print(f'✅ Extracted equityPercent: {equity_percent}%')
                    else:
                        print(f'✅ Extracted property data (no equityPercent)')
                    if matched_address and matched_address != primary_address:
                        print(f"   (Matched using: '{matched_address}')")
                    extracted_count = len([v for v in property_data.values() if v is not None])
                    print(f'   Extracted {extracted_count} field(s) total')
                    consecutive_errors = 0
            else:
                print(f'❌ API call failed - all fields set to blank')
                consecutive_errors += 1
            if consecutive_errors >= max_consecutive_errors:
                print(f'\n⚠️  Stopping: {max_consecutive_errors} consecutive errors. IP may be blocked.')
                print(f'   Processed {idx + 1}/{total} rows before stopping.')
                break
        except Exception as e:
            print(f'❌ Error processing row {idx}: {type(e).__name__}: {e}')
            print(f'   All fields set to blank')
            consecutive_errors += 1
        if idx < total - 1:
            time.sleep(delay_between)
    initial_count = len(out)
    out = out[out['estimated_equity_percentage'].notna() & (out['estimated_equity_percentage'] > 35)].copy()
    final_count = len(out)
    if verbose:
        print(f"\n{'=' * 70}")
        print(f'📊 Filtering Results:')
        print(f'   Initial rows: {initial_count}')
        print(f'   Rows with equity > 35%: {final_count}')
        print(f'   Rows removed: {initial_count - final_count}')
        print(f"{'=' * 70}")
    return out

def fetching_listings_from_density(df_json, state_codes=('AZ', 'NV', 'TX', 'CA'), density_threshold=75, out_prefix='final_zips_ordered', shuffle=True, random_state=None):
    all_results = []
    for st in state_codes:
        df_state = df_json[df_json['stusps_code'] == st].copy()
        df_zips = df_state[df_state['density'] > density_threshold].copy()
        if len(df_zips) == 0:
            print(f'No zip codes found for {st} with density > {density_threshold}')
            continue
        print(f'Processing {len(df_zips)} zip codes for {st}...')
        shuffled_zips = df_zips['zip_code'].sample(frac=0.3, random_state=random_state).reset_index(drop=True)
        print(len(shuffled_zips))
        for zip_code in shuffled_zips:
            try:
                properties = scrape_property(location=str(zip_code), listing_type='for_sale', year_built_max=2025, price_min=400000, price_max=3000000, past_days=5)
                print(len(properties))
                if len(properties) == 0:
                    continue
                properties['source_state'] = st
                properties['source_zip'] = zip_code
                all_results.append(properties)
            except Exception as e:
                print(f'Error fetching properties for zip {zip_code}: {e}')
                continue
    if len(all_results) == 0:
        print('No properties found')
        return pd.DataFrame()
    return pd.concat(all_results, ignore_index=True)

def run_homeharvest_to_csv(geojson_path: str, output_csv: str, state_codes=('AZ', 'NV', 'TX', 'CA')) -> str:
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    df_json = pd.DataFrame(data)
    df_results = fetching_listings_from_density(df_json, state_codes=state_codes)
    if len(df_results) > 0:
        df_results.to_csv(output_csv, index=False)
        print(f'Exported {len(df_results)} properties to {output_csv}')
    else:
        print('No results to export')
    return output_csv

def extract_phone_number(agent_phones):
    if pd.isna(agent_phones) or agent_phones == '':
        return None
    try:
        if isinstance(agent_phones, (list, dict)):
            phones = agent_phones if isinstance(agent_phones, list) else [agent_phones]
        elif isinstance(agent_phones, str):
            try:
                phones = ast.literal_eval(agent_phones)
                if not isinstance(phones, list):
                    phones = [phones]
            except:
                try:
                    phones = json.loads(agent_phones)
                    if not isinstance(phones, list):
                        phones = [phones]
                except:
                    return None
        else:
            return None
        for phone_obj in phones:
            if isinstance(phone_obj, dict):
                number = phone_obj.get('number') or phone_obj.get('phone') or phone_obj.get('value')
                if number:
                    return str(number)
        return None
    except Exception as e:
        return None

def map_dataframe_to_standard(df, column_mapping):
    df_mapped = df.copy()
    if 'agent_phones' in df_mapped.columns:
        df_mapped['Phone'] = df_mapped['agent_phones'].apply(extract_phone_number)
    rename_dict = {v: k for k, v in column_mapping.items() if v in df.columns and v is not None}
    df_mapped = df_mapped.rename(columns=rename_dict)
    if 'Phone' in df_mapped.columns and 'Phone' not in rename_dict.values():
        pass
    if df_mapped.columns.duplicated().any():
        df_mapped = pd.concat({c: df_mapped.loc[:, df_mapped.columns == c].bfill(axis=1).iloc[:, 0] for c in pd.unique(df_mapped.columns)}, axis=1)
    STANDARD_COLUMNS = ['Name_mls', 'Email', 'Address', 'Address Link', 'Listing Price', 'Bedroom', 'Bathroom', 'Sq Ft', 'Property Type', 'Land Type', 'Sale Type', 'Lot Size', 'Status', 'Updated', 'MLS #', 'Parking#', 'Year', 'ZIP', 'License #', 'estimated_equity_percentage', 'equity percent', 'License Link', 'Office Name', 'Phone', 'License Type', 'Name_dre', 'Mailing Address', 'Expiration Date', 'License Status', 'MLO License Endorsement', 'Salesperson License Issued', 'Broker License Issued', 'Former Name(s)', 'Main Office', 'DBA', 'Branches', 'Affiliated Licensed Corporation(s)', 'Comment', 'Disciplinary or Formal ActionDocuments', 'Licensee/Company Name', 'Mailing Address City(optional)', 'Broker Associate for', 'Former Broker Associate for', 'Salespersons', 'Responsible Broker', 'Former Responsible Broker', 'Broker Associates', 'CompanyMLO License Endorsement', 'Corporation License Issued', 'Licensed Officer(s)', 'sold_ct', 'estimatedMortgagePayment', 'negativeEquity', 'inStateAbsenteeOwner', 'absenteeOwner', 'outOfStateAbsenteeOwner', 'ownerOccupied', 'mlsDaysOnMarket', 'yearsOwned', 'propertyType', 'medianIncome', 'inherited', 'death', 'vacant', 'corporateOwned', 'investorBuyer', 'taxLien', 'judgment', 'preForeclosure', 'foreclosure', 'auction', 'reo']
    for col in STANDARD_COLUMNS:
        if col not in df_mapped.columns:
            df_mapped[col] = None
    return df_mapped.reindex(columns=STANDARD_COLUMNS)

def row_score(row):
    IMPORTANT_COLUMNS = ['Email', 'MLS #', 'License #', 'Listing Price', 'Address', 'Phone', 'Name_dre', 'License Link']
    return row[IMPORTANT_COLUMNS].notna().sum()

def choose_best_rows(df):
    df = df.copy()
    df['_score'] = df.apply(row_score, axis=1)
    best_rows = df.sort_values('_score', ascending=False).drop_duplicates(subset='Name_mls', keep='first').drop(columns='_score')
    return best_rows

def get_best_match(name1, name2, fub_names, threshold=90):
    best_score = 0
    best_match = None
    for fub_name in fub_names:
        score1 = fuzz.token_sort_ratio(str(name1), fub_name)
        score2 = fuzz.token_sort_ratio(str(name2), fub_name)
        max_score = max(score1, score2)
        if max_score > best_score and max_score >= threshold:
            best_score = max_score
            best_match = fub_name
    return best_match

def get_unmatched_rows(df, fub_name_list, fub_email_set=None, threshold=90):
    if not fub_name_list and (not fub_email_set):
        return choose_best_rows(df)
    matched_rows = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc='Matching rows'):
        name1 = row.get('Name_mls', '')
        name2 = row.get('Name_dre', '')
        email = str(row.get('Email') or '').strip().lower()
        best_match = get_best_match(name1, name2, fub_name_list, threshold=threshold)
        email_match = email in fub_email_set if fub_email_set else False
        if best_match or email_match:
            matched_rows.append(row)
    if not matched_rows:
        return choose_best_rows(df)
    matched_df = pd.DataFrame(matched_rows)
    df = df[~df['Name_mls'].isin(matched_df['Name_mls'])]
    return choose_best_rows(df)

def get_all_fub_people_with_contact_dates(API_KEY, BASE_URL='https://api.followupboss.com/v1'):
    people = []
    url = f'{BASE_URL}/people?limit=100&fields=id,name,emails,phones,lastCommunication,lastCall,lastEmail,lastText'
    pbar = tqdm(desc='Fetching FUB people')
    while url:
        pbar.update(1)
        try:
            response = requests.get(url, auth=HTTPBasicAuth(API_KEY, ''), timeout=30)
        except Exception as e:
            print(f'Error requesting FUB URL {url}: {e}')
            break
        if response.status_code != 200:
            print(f'Error from FUB: {response.status_code} - {response.text[:200]}')
            break
        try:
            data = response.json()
        except ValueError as e:
            print(f'JSON decode error from FUB on {url}: {e}. Body snippet: {response.text[:200]}')
            break
        people.extend(data.get('people', []))
        url = data.get('_metadata', {}).get('nextLink')
    pbar.close()
    return people

def get_all_copper_people(API_KEY, USER_EMAIL):
    all_people = []
    page_number = 1
    pbar = tqdm(desc='Fetching Copper people')
    HEADERS = {'X-PW-AccessToken': API_KEY, 'X-PW-Application': 'developer_api', 'X-PW-UserEmail': USER_EMAIL, 'Content-Type': 'application/json'}
    while True:
        pbar.update(1)
        response = requests.post('https://api.copper.com/developer_api/v1/people/search', headers=HEADERS, json={'page_size': 200, 'page_number': page_number, 'sort_by': 'name', 'sort_direction': 'asc'})
        if response.status_code != 200:
            print(f'Error: {response.status_code} - {response.text[:200]}')
            break
        try:
            people = response.json()
        except ValueError as e:
            print(f'JSON decode error from Copper: {e}. Body snippet: {response.text[:200]}')
            break
        if not people:
            break
        all_people.extend(people)
        page_number += 1
    pbar.close()
    return all_people

def prepare_fub_copper_recently_contacted(FUB_API_KEY=None, COPPER_API_KEY=None, COPPER_USER_EMAIL=None):
    fub_copper_names = set()
    fub_copper_emails = set()
    if FUB_API_KEY:
        print('📥 Fetching FUB data...')
        fub_people = get_all_fub_people_with_contact_dates(FUB_API_KEY)
        fub_output = []
        for person in fub_people:
            name = person.get('name')
            emails = person.get('emails', [])
            email = emails[0]['value'] if emails else None
            last_communication = person.get('lastCommunication')
            fub_output.append({'Name': name, 'Email': email, 'Last Communication': last_communication})
        FUB_api = pd.DataFrame(fub_output)
        four_months_ago = pd.Timestamp.now(tz='UTC') - pd.DateOffset(months=4)
        FUB_api['Last Communication Date'] = pd.to_datetime(FUB_api['Last Communication'].apply(lambda x: x['date'] if isinstance(x, dict) and 'date' in x else x), errors='coerce', utc=True)
        FUB_recently_contacted = FUB_api[FUB_api['Last Communication Date'] > four_months_ago]
        fub_copper_names.update(FUB_recently_contacted['Name'].dropna().unique().tolist())
        fub_copper_emails.update(FUB_recently_contacted['Email'].dropna().str.lower().unique().tolist())
        print(f'   ✅ FUB: Found {len(fub_copper_names)} unique names and {len(fub_copper_emails)} unique emails')
    if COPPER_API_KEY and COPPER_USER_EMAIL:
        print('📥 Fetching Copper data...')
        copper_people = get_all_copper_people(COPPER_API_KEY, COPPER_USER_EMAIL)
        Copper_all = pd.DataFrame(copper_people)
        if Copper_all.empty or len(Copper_all.columns) == 0:
            print(f'   ⚠️ Warning: Copper data is empty or malformed.')
            print(f'   Skipping Copper contact filtering.')
        else:
            four_months_ago = pd.Timestamp.utcnow() - pd.DateOffset(months=4)
            date_field = None
            if 'date_last_contacted' in Copper_all.columns:
                date_field = 'date_last_contacted'
            elif 'date_last_contacted_at' in Copper_all.columns:
                date_field = 'date_last_contacted_at'
            elif 'last_contacted_at' in Copper_all.columns:
                date_field = 'last_contacted_at'
            elif 'date_contacted' in Copper_all.columns:
                date_field = 'date_contacted'
            if date_field:
                Copper_all['date_last_contacted'] = pd.to_datetime(Copper_all[date_field], unit='s', utc=True, errors='coerce')
            else:
                print(f"   ⚠️ Warning: 'date_last_contacted' field not found in Copper data.")
                print(f'   Available columns: {list(Copper_all.columns)}')
                print(f'   Skipping date filtering - will include all Copper contacts.')
                Copper_all['date_last_contacted'] = pd.Timestamp('1970-01-01', tz='UTC')
            if 'emails' in Copper_all.columns:
                Copper_all['email'] = Copper_all['emails'].apply(lambda x: x[0]['email'] if isinstance(x, list) and len(x) > 0 and ('email' in x[0]) else None)
            elif 'email' in Copper_all.columns:
                Copper_all['email'] = Copper_all['email']
            elif 'email_addresses' in Copper_all.columns:
                Copper_all['email'] = Copper_all['email_addresses'].apply(lambda x: x[0]['email'] if isinstance(x, list) and len(x) > 0 and ('email' in x[0]) else x if isinstance(x, str) else None)
            else:
                print(f"   ⚠️ Warning: 'emails' field not found in Copper data.")
                print(f'   Available columns: {list(Copper_all.columns)}')
                Copper_all['email'] = None
            Copper_all_recently_contacted = Copper_all[Copper_all['date_last_contacted'] > four_months_ago]
            copper_names_before = len(fub_copper_names)
            copper_emails_before = len(fub_copper_emails)
            if 'name' in Copper_all_recently_contacted.columns:
                fub_copper_names.update(Copper_all_recently_contacted['name'].dropna().unique().tolist())
            elif 'full_name' in Copper_all_recently_contacted.columns:
                fub_copper_names.update(Copper_all_recently_contacted['full_name'].dropna().unique().tolist())
            elif 'display_name' in Copper_all_recently_contacted.columns:
                fub_copper_names.update(Copper_all_recently_contacted['display_name'].dropna().unique().tolist())
            else:
                print(f"   ⚠️ Warning: 'name' field not found in Copper data.")
            if 'email' in Copper_all_recently_contacted.columns:
                fub_copper_emails.update(Copper_all_recently_contacted['email'].dropna().str.lower().unique().tolist())
            copper_names_added = len(fub_copper_names) - copper_names_before
            copper_emails_added = len(fub_copper_emails) - copper_emails_before
            print(f'   ✅ Copper: Added {copper_names_added} unique names and {copper_emails_added} unique emails')
    return (fub_copper_names, fub_copper_emails)

def load_contacted_agents(ppath):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(ppath, scope)
    client = gspread.authorize(creds)
    sh_main = client.open_by_key(resolve_gsheet_spreadsheet_id())
    sheet = sh_main.worksheet('log')
    existing_data = sheet.get_all_records()
    all_names = set()
    all_emails = set()
    for row in existing_data:
        for name_field in ['Name_mls', 'Name_dre']:
            name = row.get(name_field)
            if name:
                all_names.add(str(name).strip())
        email = row.get('Email')
        if email:
            all_emails.add(str(email).strip().lower())
    return (all_names, all_emails)

def merge_with_existing(df_new, df_existing, key='Name_mls'):
    if df_existing.empty:
        return df_new
    df_new = df_new.copy()
    df_existing = df_existing.copy()
    df_new[key] = df_new[key].fillna('').astype(str).str.strip()
    df_existing[key] = df_existing[key].fillna('').astype(str).str.strip()
    combined = pd.concat([df_existing, df_new], ignore_index=True)
    combined['_score'] = combined.apply(row_score, axis=1)
    best = combined.sort_values('_score', ascending=False).drop_duplicates(subset=key, keep='first').drop(columns='_score')
    return best

def remove_duplicates_from_local_df(df, column_mapping, FUB_API_KEY=None, COPPER_API_KEY=None, COPPER_USER_EMAIL=None, gsheet_credentials_path=None, price_target=1500000, threshold=90):
    print('=' * 60)
    print('REMOVING DUPLICATES FROM LOCAL DATAFRAME')
    print('(No Google Sheets update - for CSV export)')
    print('=' * 60)
    print(f'\n📊 Input DataFrame shape: {df.shape}')
    print('\n📋 Step 1: Converting to standard format...')
    df_standard = map_dataframe_to_standard(df, column_mapping)
    print(f'   Shape after standardization: {df_standard.shape}')
    print('\n🔄 Step 2: Removing internal duplicates (keeping best rows)...')
    df_standard = choose_best_rows(df_standard)
    print(f'   Shape after internal deduplication: {df_standard.shape}')
    fub_copper_names = set()
    fub_copper_emails = set()
    if FUB_API_KEY or (COPPER_API_KEY and COPPER_USER_EMAIL):
        print('\n📥 Step 3: Fetching FUB/Copper recently contacted data...')
        fub_copper_names, fub_copper_emails = prepare_fub_copper_recently_contacted(FUB_API_KEY, COPPER_API_KEY, COPPER_USER_EMAIL)
        print(f'   Total unique names: {len(fub_copper_names)}')
        print(f'   Total unique emails: {len(fub_copper_emails)}')
        print('\n🔍 Step 4: Removing duplicates that exist in FUB/Copper...')
        df_before = df_standard.shape[0]
        df_standard = get_unmatched_rows(df_standard, fub_copper_names, fub_copper_emails, threshold=threshold)
        df_after = df_standard.shape[0]
        removed_count = df_before - df_after
        print(f'   ✅ Removed {removed_count} duplicates from FUB/Copper')
        print(f'   Shape after FUB/Copper filter: {df_standard.shape}')
    else:
        print('\n⏭️  Step 3: Skipping FUB/Copper filtering (no API keys provided)')
    if gsheet_credentials_path:
        print('\n📥 Step 5: Loading contacted agents from Google Sheets...')
        contacted_names, contacted_emails = load_contacted_agents(gsheet_credentials_path)
        print(f'   Found {len(contacted_names)} unique names and {len(contacted_emails)} unique emails')
        print('\n🔍 Step 6: Removing duplicates that exist in Google Sheets...')
        df_before = df_standard.shape[0]
        df_standard = get_unmatched_rows(df_standard, contacted_names, contacted_emails, threshold=threshold)
        df_after = df_standard.shape[0]
        removed_count = df_before - df_after
        print(f'   ✅ Removed {removed_count} duplicates from Google Sheets')
        print(f'   Shape after Google Sheets filter: {df_standard.shape}')
    else:
        print('\n⏭️  Step 5: Skipping Google Sheets filtering (no path provided)')
    if 'Listing Price' in df_standard.columns:
        print('\n💰 Step 7: Processing listing prices...')
        df_standard['Listing Price'] = df_standard['Listing Price'].astype(str).str.replace('[\\$,]', '', regex=True).astype(float)
        print(f"   Processed {df_standard['Listing Price'].notna().sum()} prices")
        print(f'\n🎯 Step 8: Deduplicating by email (keeping closest to ${price_target:,})...')
        df_before = df_standard.shape[0]
        df_standard['Price_Distance'] = (df_standard['Listing Price'] - price_target).abs()
        df_standard = df_standard.sort_values('Price_Distance').drop_duplicates(subset='Email', keep='first').drop(columns='Price_Distance')
        df_after = df_standard.shape[0]
        removed_count = df_before - df_after
        print(f'   ✅ Removed {removed_count} email duplicates')
        print(f'   Shape after email deduplication: {df_standard.shape}')
    print('\n' + '=' * 60)
    print(f'✅ DUPLICATE REMOVAL COMPLETE')
    print(f'   Final unique rows: {df_standard.shape[0]}')
    print(f'   Total removed: {df.shape[0] - df_standard.shape[0]}')
    print('=' * 60)
    return df_standard

def process_listings_pipeline(df, column_mapping, FUB_API_KEY=None, COPPER_API_KEY=None, COPPER_USER_EMAIL=None, gsheet_credentials_path=None, price_target=1500000, threshold=90):
    print('=' * 60)
    print('STARTING PIPELINE PROCESSING')
    print('=' * 60)
    print('\n📋 Step 1: Converting to standard format...')
    df_standard = map_dataframe_to_standard(df, column_mapping)
    print(f'   Original shape: {df.shape} → Standard shape: {df_standard.shape}')
    fub_copper_names = set()
    fub_copper_emails = set()
    if FUB_API_KEY or (COPPER_API_KEY and COPPER_USER_EMAIL):
        print('\n📥 Step 2: Fetching FUB/Copper recently contacted data...')
        fub_copper_names, fub_copper_emails = prepare_fub_copper_recently_contacted(FUB_API_KEY, COPPER_API_KEY, COPPER_USER_EMAIL)
        print(f'   Found {len(fub_copper_names)} unique names and {len(fub_copper_emails)} unique emails')
        print('\n🔍 Step 3: Filtering out FUB/Copper recently contacted...')
        df_standard = get_unmatched_rows(df_standard, fub_copper_names, fub_copper_emails, threshold=threshold)
        print(f'   After FUB/Copper filter: {df_standard.shape}')
    else:
        print('\n⏭️  Step 2: Skipping FUB/Copper filtering (no API keys provided)')
        df_standard = choose_best_rows(df_standard)
    if gsheet_credentials_path:
        print('\n📥 Step 4: Loading contacted agents from Google Sheets...')
        contacted_names, contacted_emails = load_contacted_agents(gsheet_credentials_path)
        print(f'   Found {len(contacted_names)} unique names and {len(contacted_emails)} unique emails')
        print('\n🔍 Step 5: Filtering out Google Sheets contacted...')
        df_standard = get_unmatched_rows(df_standard, contacted_names, contacted_emails, threshold=threshold)
        print(f'   After Google Sheets filter: {df_standard.shape}')
    else:
        print('\n⏭️  Step 4: Skipping Google Sheets filtering (no path provided)')
    if 'Listing Price' in df_standard.columns:
        print('\n💰 Step 6: Processing listing prices...')
        df_standard['Listing Price'] = df_standard['Listing Price'].astype(str).str.replace('[\\$,]', '', regex=True).astype(float)
        print(f"   Processed {df_standard['Listing Price'].notna().sum()} prices")
        print(f'\n🎯 Step 7: Deduplicating by email (keeping closest to ${price_target:,})...')
        df_standard['Price_Distance'] = (df_standard['Listing Price'] - price_target).abs()
        df_standard = df_standard.sort_values('Price_Distance').drop_duplicates(subset='Email', keep='first').drop(columns='Price_Distance')
        print(f'   After deduplication: {df_standard.shape}')
    print('\n' + '=' * 60)
    print(f'✅ PIPELINE COMPLETE')
    print(f'   Final shape: {df_standard.shape[0]}')
    print('=' * 60)
    return df_standard

def update_sheet(df: pd.DataFrame, ppath: str=None) -> None:
    KEEP_COLUMNS = ['Name_mls', 'Email', 'Address', 'Address Link', 'Listing Price', 'Bedroom', 'Bathroom', 'Sq Ft', 'Property Type', 'Land Type', 'Sale Type', 'Lot Size', 'Status', 'Updated', 'MLS #', 'Parking#', 'Year', 'ZIP', 'License #', 'estimated_equity_percentage', 'equity percent', 'License Link', 'Office Name', 'Phone', 'License Type', 'Name_dre', 'Mailing Address', 'Expiration Date', 'License Status', 'MLO License Endorsement', 'Salesperson License Issued', 'Broker License Issued', 'Former Name(s)', 'Main Office', 'DBA', 'Branches', 'Affiliated Licensed Corporation(s)', 'Comment', 'Disciplinary or Formal ActionDocuments', 'Licensee/Company Name', 'Mailing Address City(optional)', 'Broker Associate for', 'Former Broker Associate for', 'Salespersons', 'Responsible Broker', 'Former Responsible Broker', 'Broker Associates', 'CompanyMLO License Endorsement', 'Corporation License Issued', 'Licensed Officer(s)', 'sold_ct', 'estimatedMortgagePayment', 'negativeEquity', 'inStateAbsenteeOwner', 'absenteeOwner', 'outOfStateAbsenteeOwner', 'ownerOccupied', 'mlsDaysOnMarket', 'yearsOwned', 'propertyType', 'medianIncome', 'inherited', 'death', 'vacant', 'corporateOwned', 'investorBuyer', 'taxLien', 'judgment', 'preForeclosure', 'foreclosure', 'auction', 'reo']
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    if ppath is None:
        ppath = resolve_gsheet_credentials_path()
    creds = ServiceAccountCredentials.from_json_keyfile_name(ppath, scope)
    client = gspread.authorize(creds)
    sh_main = client.open_by_key(resolve_gsheet_spreadsheet_id())
    sheet1 = sh_main.worksheet('log')
    try:
        existing_data = sheet1.get_all_records()
        sheet_empty = len(existing_data) == 0
    except:
        existing_data = []
        sheet_empty = True
    existing_df = pd.DataFrame(existing_data)
    if sheet_empty or existing_df.empty:
        print("📋 Sheet 'log' is empty - creating with all columns")
        existing_df = pd.DataFrame(columns=KEEP_COLUMNS)
    else:
        for col in KEEP_COLUMNS:
            if col not in existing_df.columns:
                existing_df[col] = ''
        existing_df = existing_df.reindex(columns=KEEP_COLUMNS, fill_value='')
        print(f'📋 Added {len([c for c in KEEP_COLUMNS if c not in pd.DataFrame(existing_data).columns])} new column(s) to existing data')
    if 'MLS #' not in df.columns:
        df['MLS #'] = ''
    df = df.reindex(columns=KEEP_COLUMNS, fill_value='')
    df['MLS #'] = df['MLS #'].astype(str).str.strip()
    existing_df['MLS #'] = existing_df['MLS #'].astype(str).str.strip()
    existing_df = existing_df[~existing_df['MLS #'].isin(df['MLS #'])]
    all_rows = pd.concat([existing_df, df], ignore_index=True)
    sheet1.clear()
    sheet1.update(range_name='A1', values=[KEEP_COLUMNS])
    set_with_dataframe(sheet1, all_rows, row=2, include_column_header=False)
    print(f'✅ Sheet1 (log) updated with {len(all_rows)} rows (existing: {len(existing_df)}, new: {len(df)})')
    try:
        sheet2 = sh_main.worksheet('weekly')
    except gspread.WorksheetNotFound:
        sheet2 = sh_main.add_worksheet('weekly', rows='1000', cols='50')
    sheet2.clear()
    sheet2.update(range_name='A1', values=[KEEP_COLUMNS])
    set_with_dataframe(sheet2, df, row=2, include_column_header=False)
    print(f'✅ Sheet2 (weekly) overwritten with {len(df)} rows.')
    try:
        sheet_contacted = sh_main.worksheet('contacted')
    except gspread.WorksheetNotFound:
        sheet_contacted = sh_main.add_worksheet('contacted', rows='1000', cols='50')
    try:
        existing_contacted_values = sheet_contacted.get_all_values()
        has_headers = len(existing_contacted_values) > 0
    except:
        has_headers = False
    if not has_headers:
        sheet_contacted.update(range_name='A1', values=[KEEP_COLUMNS])
        print("📋 Added headers to 'contacted' sheet")
    else:
        existing_contacted_df = pd.DataFrame(existing_contacted_values[1:], columns=existing_contacted_values[0])
        missing_cols = [c for c in KEEP_COLUMNS if c not in existing_contacted_df.columns]
        if missing_cols:
            for col in missing_cols:
                existing_contacted_df[col] = ''
            existing_contacted_df = existing_contacted_df.reindex(columns=KEEP_COLUMNS, fill_value='')
            sheet_contacted.clear()
            sheet_contacted.update(range_name='A1', values=[KEEP_COLUMNS])
            set_with_dataframe(sheet_contacted, existing_contacted_df, row=2, include_column_header=False)
            print(f"📋 Added {len(missing_cols)} new column(s) to 'contacted' sheet")
    existing_row_count = len(sheet_contacted.get_all_values())
    start_row = existing_row_count + 1 if has_headers else 2
    set_with_dataframe(sheet_contacted, df, row=start_row, include_column_header=False)
    print(f"✅ Sheet 'contacted' appended with {len(df)} new rows (total rows now: {existing_row_count + len(df)}).")
    print('✔️ All updates complete.')
FILTERED_LISTINGS_MAPPING = {'Name_mls': 'agent_name', 'Name_dre': None, 'Email': 'agent_email', 'Address': 'formatted_address', 'Address Link': 'property_url', 'Listing Price': 'list_price', 'Bedroom': 'beds', 'Bathroom': 'full_baths', 'MLS #': 'mls_id', 'ZIP': 'zip_code', 'Sq Ft': 'sqft', 'Lot Size': 'lot_sqft', 'Property Type': 'style', 'Land Type': None, 'Year': 'year_built', 'Updated': 'last_update_date', 'Status': 'status', 'License #': 'agent_nrds_id', 'equity percent': 'estimated_equity_percentage'}

def _default_creds_path() -> str:
    return resolve_gsheet_credentials_path()


DEFAULT_PIPELINE_STEPS = ("fetch", "process", "enrich", "push")
VALID_STEPS = frozenset({"fetch", "process", "enrich", "push", "push-file"})


@dataclass
class PipelineContext:
    geojson: str
    homeharvest_csv: str
    mls_unique_csv: str
    price_target: float
    threshold: int
    enriched_csv: Optional[str] = None


def _build_pipeline_context() -> PipelineContext:
    enc = os.environ.get("ENRICHED_CSV", "").strip() or None
    return PipelineContext(
        geojson=resolve_georef_json(),
        homeharvest_csv=os.environ.get("HOMEHARVEST_CSV", "homeharvest_results.csv").strip(),
        mls_unique_csv=os.environ.get("MLS_UNIQUE_CSV", "MLS_unique.csv").strip(),
        price_target=float(os.environ.get("SCRAPE_PRICE_TARGET", "1500000")),
        threshold=int(os.environ.get("SCRAPE_MATCH_THRESHOLD", "90")),
        enriched_csv=enc,
    )


def _steps_from_env() -> List[str]:
    raw = os.environ.get("SCRAPE_STEPS", "fetch,process,enrich,push")
    out = [s.strip().lower() for s in raw.split(",") if s.strip()]
    if not out:
        raise SystemExit(
            "SCRAPE_STEPS is empty; set e.g. fetch,process,enrich,push in .env"
        )
    for s in out:
        if s not in VALID_STEPS:
            raise SystemExit(
                f"Invalid SCRAPE_STEPS entry: {s!r} (valid: {sorted(VALID_STEPS)})"
            )
    return out


def _resolve_steps_from_argv() -> Optional[List[str]]:
    if len(sys.argv) <= 1:
        return None
    cmd = sys.argv[1].lower()
    if cmd in ("-h", "--help"):
        return None
    if cmd == "all":
        return list(DEFAULT_PIPELINE_STEPS)
    if cmd not in VALID_STEPS:
        print(
            f"Unknown command {sys.argv[1]!r}. Use: {', '.join(sorted(VALID_STEPS))}, or all",
            file=sys.stderr,
        )
        sys.exit(2)
    return [cmd]


def _print_cli_help() -> None:
    print(
        """scrape.py — MLS pipeline

  python scrape.py
      Run steps from SCRAPE_STEPS in .env (default: fetch,process,enrich,push).

  python scrape.py <step>|all
      Optional: run one step only, or \"all\" for fetch,process,enrich,push.

  python scrape.py -h | --help
      Show this message.

Place zoomcasa-scaler-key1-5b442b14e7cd.json in this directory (or set GSHEET_* in .env).
Set FUB/Copper/Primetracers and paths in .env — see .env.example.
"""
    )


def _run_fetch(ctx: PipelineContext) -> None:
    run_homeharvest_to_csv(ctx.geojson, ctx.homeharvest_csv)


def _run_process(ctx: PipelineContext, creds_path: str) -> None:
    fub = os.environ.get("FUB_API_KEY")
    copper_k = os.environ.get("COPPER_API_KEY")
    copper_u = os.environ.get("COPPER_USER_EMAIL")
    df_results = pd.read_csv(ctx.homeharvest_csv)
    df_unique = remove_duplicates_from_local_df(
        df=df_results,
        column_mapping=FILTERED_LISTINGS_MAPPING,
        FUB_API_KEY=fub,
        COPPER_API_KEY=copper_k,
        COPPER_USER_EMAIL=copper_u,
        gsheet_credentials_path=creds_path,
        price_target=ctx.price_target,
        threshold=ctx.threshold,
    )
    df_unique.to_csv(ctx.mls_unique_csv, index=False)
    print(f"Exported {len(df_unique)} rows to {ctx.mls_unique_csv}")


def _run_enrich(ctx: PipelineContext) -> None:
    df_unique = pd.read_csv(ctx.mls_unique_csv)
    sampled = enrich_sampled_merged_with_primetracers(
        sampled_merged=df_unique,
        client_uuid=os.environ.get("PRIMETRACERS_CLIENT_UUID"),
        delay_between=float(os.environ.get("PRIMETRACERS_DELAY", "2.5")),
        use_fallback=True,
        start_idx=int(os.environ.get("PRIMETRACERS_START_IDX", "0")),
        verbose=True,
    )
    ts = datetime.now().strftime("%Y%m%d")
    out = f"sampled_merged_with_primetracers_{ts}.csv"
    sampled.to_csv(out, index=False)
    ctx.enriched_csv = out
    print(f"Saved {out}")


def _enriched_path_for_push(ctx: PipelineContext) -> str:
    if ctx.enriched_csv:
        return ctx.enriched_csv
    ts = datetime.now().strftime("%Y%m%d")
    return f"sampled_merged_with_primetracers_{ts}.csv"


def _run_push(ctx: PipelineContext) -> None:
    path = _enriched_path_for_push(ctx)
    df_unique = pd.read_csv(path)
    cols = [c for c in df_unique.columns if c != "estimated_equity_percentage"]
    zip_idx = cols.index("ZIP")
    cols.insert(zip_idx + 1, "estimated_equity_percentage")
    df_unique = df_unique[cols]
    df_unique = df_unique.drop_duplicates(subset="MLS #", keep="first")
    update_sheet(df_unique, ppath=_default_creds_path())


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        _print_cli_help()
        return
    steps = _resolve_steps_from_argv()
    if steps is None:
        steps = _steps_from_env()
    creds_path = resolve_gsheet_credentials_path()
    ctx = _build_pipeline_context()
    for raw in steps:
        step = "push" if raw == "push-file" else raw
        if step == "fetch":
            _run_fetch(ctx)
        elif step == "process":
            _run_process(ctx, creds_path)
        elif step == "enrich":
            _run_enrich(ctx)
        elif step == "push":
            _run_push(ctx)


if __name__ == "__main__":
    main()
