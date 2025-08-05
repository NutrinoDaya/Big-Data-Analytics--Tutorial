# test_analytics_api.py
import requests

BASE_URL = "http://127.0.0.1:8000"

def test_summary():
    url = f"{BASE_URL}/analytics/summary"
    resp = requests.get(url)
    print("Summary Statistics:")
    print(resp.status_code, resp.json())

def test_top_pickups(limit=5):
    url = f"{BASE_URL}/analytics/top-pickup-locations"
    params = {"limit": limit}
    resp = requests.get(url, params=params)
    print(f"Top {limit} Pickup Locations:")
    print(resp.status_code, resp.json())

if __name__ == "__main__":
    test_summary()
    test_top_pickups(5)