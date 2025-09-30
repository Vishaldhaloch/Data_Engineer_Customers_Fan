import requests
import json

BASE_URL = "http://localhost:8000"

def pretty_print(title, data):
    print(f"\n=== {title} ===")
    print(json.dumps(data, indent=4))

def test_health():
    r = requests.get(f"{BASE_URL}/health")
    pretty_print("Health Check", r.json())

def test_list_customers(page=1, page_size=10, country=None, q=None):
    params = {"page": page, "page_size": page_size}
    if country:
        params["country"] = country
    if q:
        params["q"] = q
    r = requests.get(f"{BASE_URL}/customers", params=params)
    pretty_print("List Customers", r.json())

def test_customer_profile(customer_id="user-1"):
    r = requests.get(f"{BASE_URL}/customers/{customer_id}")
    pretty_print(f"Customer Profile: {customer_id}", r.json())

def test_fan_score(customer_id="user-1"):
    r = requests.get(f"{BASE_URL}/fan-score/{customer_id}")
    pretty_print(f"Fan Score: {customer_id}", r.json())

def test_fanbase_countries(top=5):
    r = requests.get(f"{BASE_URL}/fanbase/countries", params={"top": top})
    pretty_print("Fanbase by Country", r.json())

def test_orders(customer_id=None, from_ts=None, to_ts=None, activity_type=None):
    params = {}
    if customer_id:
        params["customer_id"] = customer_id
    if from_ts:
        params["from_ts"] = from_ts
    if to_ts:
        params["to_ts"] = to_ts
    if activity_type:
        params["activity_type"] = activity_type
    r = requests.get(f"{BASE_URL}/orders", params=params)
    pretty_print("Orders", r.json())

if __name__ == "__main__":
    test_health()
    test_list_customers(page=1, page_size=5, country="United States")
    test_customer_profile(customer_id="user-1")
    test_fan_score(customer_id="user-1")
    test_fanbase_countries(top=5)
    test_orders(customer_id="user-1", activity_type="purchase")
