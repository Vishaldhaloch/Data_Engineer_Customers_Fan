import requests, json
from etl.db import get_conn
from etl.cleaning import normalize_email, normalize_name, normalize_country, phone_to_e164

DUMMY_URL = "https://dummyjson.com/users"

def upsert_customer_record(user):
    customer_id = f"user-{user['id']}"
    email = normalize_email(user.get('email'))
    first_name = normalize_name(user.get('firstName'))
    last_name = normalize_name(user.get('lastName'))
    addr = user.get('address', {}) or {}
    country = normalize_country(addr.get('country'))
    state = addr.get('state')
    city = addr.get('city')
    postcode = addr.get('postalCode')
    dob = user.get('birthDate')
    phone = phone_to_e164(user.get('phone'), country)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
INSERT INTO core.customer
 (customer_id, first_name, last_name, email, phone_e164, country, state, city, postcode, dob, created_at, updated_at)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
 ON CONFLICT (customer_id) DO UPDATE
 SET first_name = EXCLUDED.first_name,
     last_name = EXCLUDED.last_name,
     email = EXCLUDED.email,
     phone_e164 = EXCLUDED.phone_e164,
     country = EXCLUDED.country,
     state = EXCLUDED.state,
     city = EXCLUDED.city,
     postcode = EXCLUDED.postcode,
     dob = EXCLUDED.dob,
     updated_at = CASE
       WHEN core.customer.first_name IS DISTINCT FROM EXCLUDED.first_name
         OR core.customer.last_name IS DISTINCT FROM EXCLUDED.last_name
         OR core.customer.email IS DISTINCT FROM EXCLUDED.email
         OR core.customer.phone_e164 IS DISTINCT FROM EXCLUDED.phone_e164
         OR core.customer.country IS DISTINCT FROM EXCLUDED.country
         OR core.customer.state IS DISTINCT FROM EXCLUDED.state
         OR core.customer.city IS DISTINCT FROM EXCLUDED.city
         OR core.customer.postcode IS DISTINCT FROM EXCLUDED.postcode
         OR core.customer.dob IS DISTINCT FROM EXCLUDED.dob
       THEN now() ELSE core.customer.updated_at END
""", (customer_id, first_name, last_name, email, phone, country, state, city, postcode, dob))

def fetch_all_and_load():
    skip = 0
    limit = 10
    while True:
        resp = requests.get(f"{DUMMY_URL}?limit={limit}&skip={skip}")
        resp.raise_for_status()
        page = resp.json()
        users = page.get('users', [])
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO landing.customers_raw (payload) VALUES (%s)", (json.dumps(page),))
        if not users:
            break
        for u in users:
            upsert_customer_record(u)
        skip += limit

if __name__ == "__main__":
    fetch_all_and_load()
