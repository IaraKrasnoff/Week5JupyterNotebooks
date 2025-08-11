#!/usr/bin/env python3
"""
NOAA Climate Data API Location Fetcher - TEST VERSION

This is a test version that only makes 3-4 requests to verify your setup
before running the full script.
"""

import urllib.request
import json
import time
import os

# REPLACE WITH YOUR ACTUAL NOAA API TOKEN
TOKEN = "HHBZSMQFbcmOLuNpgrmyyCJTNOhQNpxH"

# API Configuration
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/locations"
LIMIT = 1000
TEST_REQUESTS = 3  # Only make 3 requests for testing

def make_api_request(offset, limit):
    """Make a request to NOAA API for locations data"""
    url = f"{BASE_URL}?limit={limit}&offset={offset}"
    
    request = urllib.request.Request(url)
    request.add_header('token', TOKEN)
    
    try:
        with urllib.request.urlopen(request) as response:
            data = json.loads(response.read().decode())
            return data
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.reason}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def save_to_file(data, file_index):
    """Save JSON data to a file"""
    # Create data directory if it doesn't exist
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    
    filename = os.path.join(data_dir, f"locations_{file_index}.json")
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"✓ Saved {filename}")

def main():
    print("NOAA API Test Script")
    print(f"Making {TEST_REQUESTS} test requests...")
    print("=" * 40)
    
    if TOKEN == "YOUR_NOAA_API_TOKEN_HERE":
        print("❌ ERROR: Please set your NOAA API token!")
        return
    
    for i in range(TEST_REQUESTS):
        offset = i * LIMIT + 1
        print(f"\nTest Request {i + 1}: offset={offset}")
        
        data = make_api_request(offset, LIMIT)
        
        if data:
            save_to_file(data, i)
            results_count = len(data.get('results', []))
            print(f"  Got {results_count} results")
        else:
            print(f"❌ Request failed")
        
        if i < TEST_REQUESTS - 1:
            time.sleep(1)  # Wait between requests
    
    print(f"\n✓ Test completed! If this worked, you can run the full script.")

if __name__ == "__main__":
    main()
