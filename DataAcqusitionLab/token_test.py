#!/usr/bin/env python3
"""
Simple NOAA API Token Test
Tests if your token is valid with a minimal request
"""

import urllib.request
import json

# Your token here
TOKEN = "HHBZSMQFbcmOLuNpgrmyyCJTNOhQNpxH"  # Replace with your real token

def test_token():
    """Test if the token works with a simple request"""
    # Simple request - just get a few locations without offset
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/locations?limit=5"
    
    print(f"Testing token: {TOKEN[:8]}...")
    print(f"URL: {url}")
    
    request = urllib.request.Request(url)
    request.add_header('token', TOKEN)
    
    try:
        with urllib.request.urlopen(request) as response:
            print(f"✅ SUCCESS! Status: {response.status}")
            data = json.loads(response.read().decode())
            
            if 'results' in data:
                print(f"✅ Got {len(data['results'])} results")
                print("✅ Your token is working!")
                return True
            else:
                print("❌ No results in response")
                return False
                
    except urllib.error.HTTPError as e:
        print(f"❌ HTTP Error {e.code}: {e.reason}")
        if e.code == 400:
            print("   This usually means invalid token or bad request format")
        elif e.code == 401:
            print("   This means unauthorized - check your token")
        elif e.code == 429:
            print("   This means too many requests - wait and try again")
        return False
    except Exception as e:
        print(f"❌ Other error: {e}")
        return False

if __name__ == "__main__":
    print("NOAA API Token Test")
    print("=" * 30)
    
    if TOKEN == "abcd1234efgh5678ijkl9012mnop3456":
        print("❌ You're still using the placeholder token!")
        print("🔑 Get a real token from: https://www.ncdc.noaa.gov/cdo-web/webservices/v2#gettingStarted")
    else:
        test_token()
