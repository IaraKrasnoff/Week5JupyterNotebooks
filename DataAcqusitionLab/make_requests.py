#!/usr/bin/env python3
"""
Improved NOAA API Location Fetcher with Error Handling
This version handles API failures better and can resume from where it left off
"""

import urllib.request
import json
import time
import os

# Your NOAA API token
TOKEN = "HHBZSMQFbcmOLuNpgrmyyCJTNOhQNpxH"

# API Configuration
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/locations"
LIMIT = 1000
TOTAL_RECORDS = 38859
DELAY_BETWEEN_REQUESTS = 2  # Increased delay to be nicer to API
MAX_RETRIES = 3  # Number of retries for failed requests

def make_api_request(offset, limit, retry_count=0):
    """Make a request with retry logic"""
    url = f"{BASE_URL}?limit={limit}&offset={offset}"
    
    request = urllib.request.Request(url)
    request.add_header('token', TOKEN)
    
    try:
        print(f"  Making request to offset {offset}...")
        with urllib.request.urlopen(request) as response:
            data = json.loads(response.read().decode())
            return data, None
            
    except urllib.error.HTTPError as e:
        error_msg = f"HTTP Error {e.code}: {e.reason}"
        print(f"  {error_msg}")
        
        # Handle specific error codes
        if e.code == 503:  # Service Unavailable
            if retry_count < MAX_RETRIES:
                wait_time = (retry_count + 1) * 5  # Exponential backoff
                print(f"  Server unavailable. Waiting {wait_time} seconds before retry {retry_count + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                return make_api_request(offset, limit, retry_count + 1)
            else:
                return None, f"Max retries exceeded: {error_msg}"
        elif e.code == 429:  # Too Many Requests
            wait_time = 30
            print(f"  Rate limited. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            if retry_count < MAX_RETRIES:
                return make_api_request(offset, limit, retry_count + 1)
        
        return None, error_msg
        
    except Exception as e:
        error_msg = f"Network error: {e}"
        print(f"  {error_msg}")
        
        if retry_count < MAX_RETRIES:
            wait_time = 5
            print(f"  Retrying in {wait_time} seconds... ({retry_count + 1}/{MAX_RETRIES})")
            time.sleep(wait_time)
            return make_api_request(offset, limit, retry_count + 1)
        
        return None, error_msg

def save_to_file(data, file_index):
    """Save JSON data to file"""
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    
    filename = os.path.join(data_dir, f"locations_{file_index}.json")
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"  Saved {filename}")
        return True
    except Exception as e:
        print(f"  Error saving {filename}: {e}")
        return False

def check_existing_files():
    """Check what files already exist"""
    data_dir = "data"
    if not os.path.exists(data_dir):
        return []
    
    existing_files = []
    for i in range(39):  # 0 to 38
        filename = os.path.join(data_dir, f"locations_{i}.json")
        if os.path.exists(filename):
            existing_files.append(i)
    
    return existing_files

def main():
    print("NOAA Climate Data API Location Fetcher")
    print("=" * 60)
    
    # Check existing files
    existing_files = check_existing_files()
    if existing_files:
        print(f"Found {len(existing_files)} existing files: {existing_files}")
        if len(existing_files) == 39:
            print("All 39 files already exist. Skipping download and showing summary.")
        else:
            print(f"Continuing from where we left off. Need {39 - len(existing_files)} more files.")
    else:
        print("No existing files found. Starting fresh.")
    
    num_requests = (TOTAL_RECORDS + LIMIT - 1) // LIMIT  # 39 requests total
    print(f"Total requests needed: {num_requests}")
    print(f"Retry limit per request: {MAX_RETRIES}")
    print(f"Delay between requests: {DELAY_BETWEEN_REQUESTS}s")
    print("=" * 60)
    
    successful_requests = 0
    failed_requests = []
    
    for i in range(num_requests):
        # Skip if file already exists
        if i in existing_files:
            print(f"\nRequest {i + 1}/{num_requests}: Skipping (file exists)")
            successful_requests += 1
            continue
            
        offset = i * LIMIT + 1
        print(f"\nRequest {i + 1}/{num_requests}: Fetching records {offset}-{min(offset + LIMIT - 1, TOTAL_RECORDS)}")
        
        # Make API request with retries
        data, error = make_api_request(offset, LIMIT)
        
        if data and 'results' in data:
            # Save to file
            if save_to_file(data, i):
                successful_requests += 1
                records_count = len(data['results'])
                print(f"  Records saved: {records_count}")
            else:
                failed_requests.append((i, "File save error"))
        else:
            failed_requests.append((i, error or "Unknown error"))
            print(f"  Request {i + 1} failed permanently")
        
        # Progress update
        print(f"  Progress: {successful_requests}/{num_requests} completed")
        
        # Delay between requests (except for the last one)
        if i < num_requests - 1:
            print(f"  Waiting {DELAY_BETWEEN_REQUESTS} seconds...")
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # Final summary
    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    print(f"Successful requests: {successful_requests}/{num_requests}")
    print(f"Failed requests: {len(failed_requests)}")
    
    if failed_requests:
        print("\nFailed requests:")
        for file_index, error in failed_requests:
            print(f"  - locations_{file_index}.json: {error}")
        print(f"\nYou can run this script again to retry failed requests!")
    
    # List created files
    final_files = check_existing_files()
    print(f"\nTotal files created: {len(final_files)}")
    if len(final_files) > 0:
        print("Created files:")
        for file_index in sorted(final_files)[:10]:  # Show first 10
            print(f"  data/locations_{file_index}.json")
        if len(final_files) > 10:
            print(f"  ... and {len(final_files) - 10} more files")

if __name__ == "__main__":
    main()
