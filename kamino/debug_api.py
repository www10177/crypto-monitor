import requests
import json

# Main Market PubKey (from previous logs/searches)
MARKET_PUBKEY = "7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF"
# User's Sol Resolved Address (from logs)
USER_PUBKEY = "CSciTEoppsvXqRN3ZogYKgGgHdS6FnPRBhwxGP3T7EzH"

def debug_kamino():
    url = f"https://api.kamino.finance/kamino-market/{MARKET_PUBKEY}/users/{USER_PUBKEY}/obligations"
    print(f"Fetching: {url}")
    
    try:
        resp = requests.get(url, timeout=10)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            print(json.dumps(data, indent=2))
        else:
            print(resp.text)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_kamino()
