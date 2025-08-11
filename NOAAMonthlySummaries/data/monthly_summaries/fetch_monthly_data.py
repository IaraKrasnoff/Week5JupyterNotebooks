#!/usr/bin/env python3
"""
NOAA Monthly Summaries Data Fetcher
Fetches Global Summary of the Month (GSOM) data for TAVG (average temperature)
for FIPS:10003 (New Castle County, DE) from 1938-2018 (80+ years)
"""

import requests
import json
import os
import time
from datetime import datetime

# Configuration
TOKEN = "HHBZSMQFbcmOLuNpgrmyyCJTNOhQNpxH"  # Your NOAA API token
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
DATA_DIR = "."  # Save files in the current directory (data/monthly_summaries)

# API Parameters
DATASET_ID = "GSOM"
LOCATION_ID = "FIPS:10003"  # New Castle County, Delaware
DATATYPE_ID = "TAVG"        # Average temperature
LIMIT = 1000
OFFSET = 1

# Date ranges (10-year chunks for 80+ years: 1938-2018)
START_YEAR = 1938
END_YEAR = 2018
CHUNK_SIZE = 10  # 10-year periods

def create_date_ranges():
    """Create list of date ranges for API requests"""
    ranges = []
    current_year = START_YEAR
    
    while current_year <= END_YEAR:
        end_year = min(current_year + CHUNK_SIZE - 1, END_YEAR)
        start_date = f"{current_year}-01-01"
        end_date = f"{end_year}-12-31"
        
        ranges.append({
            'start_year': current_year,
            'end_year': end_year,
            'start_date': start_date,
            'end_date': end_date,
            'filename': f"FIPS10003_avg_{current_year}_to_{end_year}.json"
        })
        
        current_year += CHUNK_SIZE
    
    return ranges

def make_api_request(start_date, end_date):
    """Make API request to NOAA for monthly data"""
    headers = {"token": TOKEN}
    params = {
        "datasetid": DATASET_ID,
        "locationid": LOCATION_ID,
        "datatypeid": DATATYPE_ID,
        "startdate": start_date,
        "enddate": end_date,
        "limit": LIMIT,
        "offset": OFFSET
    }
    
    try:
        print(f"  Making API request for {start_date} to {end_date}...")
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        if response.content:
            # Adjusted to match the new response structure
            data = response.json()
            adjusted_results = []
            
            for item in data.get('results', []):
                adjusted_item = {
                    "date": item.get("date"),
                    "datatype": item.get("datatype"),
                    "station": item.get("station"),
                    "attributes": item.get("attributes"),
                    "value": item.get("value")
                }
                adjusted_results.append(adjusted_item)
            
            data['results'] = adjusted_results
            
            return data, None
        else:
            return None, "Empty response"
            
    except requests.exceptions.RequestException as e:
        return None, f"Request error: {e}"
    except json.JSONDecodeError as e:
        return None, f"JSON decode error: {e}"

def save_data(data, filename):
    """Save data to JSON file"""
    filepath = os.path.join(DATA_DIR, filename)
    print(f"  DEBUG: Trying to save to: {os.path.abspath(filepath)}")  # Add this line
    
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        
        # Count records if available
        record_count = len(data.get('results', [])) if data and 'results' in data else 0
        print(f"  Saved: {filename} ({record_count} records)")
        return True
        
    except Exception as e:
        print(f"  Error saving {filename}: {e}")
        return False

def check_existing_files():
    """Check what files already exist"""
    if not os.path.exists(DATA_DIR):
        return []
    
    existing = []
    for filename in os.listdir(DATA_DIR):
        if filename.startswith("FIPS10003_avg_") and filename.endswith(".json"):
            existing.append(filename)
    
    return existing

def main():
    print("NOAA Monthly Summaries Data Fetcher")
    print("=" * 50)
    print(f"Dataset: {DATASET_ID} (Global Summary of the Month)")
    print(f"Location: {LOCATION_ID} (New Castle County, DE)")
    print(f"Data Type: {DATATYPE_ID} (Average Temperature)")
    print(f"Period: {START_YEAR}-{END_YEAR} ({END_YEAR - START_YEAR + 1} years)")
    print(f"Data Directory: {DATA_DIR}")
    print("=" * 50)
    
    # Create data directory
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Check existing files
    existing_files = check_existing_files()
    if existing_files:
        print(f"Found {len(existing_files)} existing files:")
        for filename in sorted(existing_files):
            print(f"  - {filename}")
        print()
    
    # Generate date ranges
    date_ranges = create_date_ranges()
    print(f"Will process {len(date_ranges)} date ranges:")
    
    successful = 0
    failed = []
    
    for i, range_info in enumerate(date_ranges, 1):
        filename = range_info['filename']
        
        # Skip if file already exists
        if filename in existing_files:
            print(f"Request {i}/{len(date_ranges)}: Skipping {filename} (already exists)")
            successful += 1
            continue
        
        print(f"Request {i}/{len(date_ranges)}: {range_info['start_date']} to {range_info['end_date']}")
        
        # Make API request
        data, error = make_api_request(range_info['start_date'], range_info['end_date'])
        
        if data:
            if save_data(data, filename):
                successful += 1
            else:
                failed.append(filename)
        else:
            print(f"  Failed: {error}")
            failed.append(filename)
        
        # Delay between requests to be nice to the API
        if i < len(date_ranges):
            print("  Waiting 2 seconds...")
            time.sleep(2)
    
    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total requests: {len(date_ranges)}")
    print(f"Successful: {successful}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print(f"Failed files: {failed}")
    
    print(f"\nData saved to: {os.path.abspath(DATA_DIR)}")
    
    # List final files
    final_files = check_existing_files()
    if final_files:
        print(f"\nFinal files ({len(final_files)}):")
        for filename in sorted(final_files):
            filepath = os.path.join(DATA_DIR, filename)
            size = os.path.getsize(filepath)
            print(f"  - {filename} ({size:,} bytes)")

if __name__ == "__main__":
    main()