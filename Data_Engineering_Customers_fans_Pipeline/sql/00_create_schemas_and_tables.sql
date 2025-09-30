CREATE EXTENSION IF NOT EXISTS citext;

CREATE SCHEMA IF NOT EXISTS landing;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- landing
CREATE TABLE IF NOT EXISTS landing.customers_raw (
  ingested_at timestamptz DEFAULT now(),
  payload jsonb
);

CREATE TABLE IF NOT EXISTS landing.orders_activity_raw (
  ingested_at timestamptz DEFAULT now(),
  payload xml
);

-- core.customer
CREATE TABLE IF NOT EXISTS core.customer (
  customer_id text PRIMARY KEY,
  first_name text,
  last_name text,
  email citext UNIQUE,
  phone_e164 text,
  country text,
  state text,
  city text,
  postcode text,
  dob date,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- core.order_activity
CREATE TABLE IF NOT EXISTS core.order_activity (
  order_id text PRIMARY KEY,
  customer_id text REFERENCES core.customer(customer_id),
  order_ts timestamptz,
  activity_type text,
  channel text,
  items int,
  total_amount numeric(12,2),
  currency text,
  created_at timestamptz DEFAULT now()
);

-- core.load_rejects
CREATE TABLE IF NOT EXISTS core.load_rejects (
  id bigserial PRIMARY KEY,
  context text,
  natural_key text,
  reason text,
  payload jsonb,
  rejected_at timestamptz DEFAULT now()
);

-- static currency rates (small static table required by assignment)
CREATE TABLE IF NOT EXISTS core.currency_rates (
  currency_code text PRIMARY KEY,
  rate_to_usd numeric(12,6)
);

TRUNCATE core.currency_rates;

INSERT INTO core.currency_rates (currency_code, rate_to_usd) VALUES
  ('USD', 1.000000),
  ('GBP', 1.250000),
  ('EUR', 1.100000),
  ('INR', 0.012000),
  ('AUD', 0.650000),
  ('CAD', 0.750000);
