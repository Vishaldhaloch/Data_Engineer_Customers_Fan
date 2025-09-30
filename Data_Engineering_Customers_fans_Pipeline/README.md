Customers→Fans Pipeline
Overview

This project implements a data pipeline for loading customer and order data, calculating fan scores, and exposing the results via a FastAPI read-only API.

Key Features:

Fetch customers via paged API calls → cleaned → loaded into core.customer.

Load orders from provided XML → validated → inserted into core.order_activity.

Invalid orders stored in core.load_rejects.

Prebuilt materialized views for fan score (mart.v_customer_fan_score) and fanbase by country (mart.v_fanbase_by_country).

FastAPI endpoints with filtering, pagination, and structured responses.

ETL is idempotent: safe to rerun without duplicating records.

Repository Structure

customers-fans-pipeline/
├─ api/
│  └─ main.py                # FastAPI app
├─ etl/
│  ├─ fetch_customers.py     # ETL: fetch customers via API
│  ├─ load_orders.py         # ETL: load XML orders
│  ├─ db.py                  # DB helper functions (optional)
│  └─ cleaning.py            # Utilities: clean emails, phones, names, locations
├─ sql/
│  ├─ 00_create_schemas_and_tables.sql  # Schemas & tables
│  └─ 01_mart_views.sql                  # Views for fan scores & fanbase
├─ orders_activity.xml       # Provided orders XML
├─ requirements.txt          # Python dependencies
├─ api_test_client.py        # Optional: test all API endpoints
└─ README.md                 # Project documentation


⚙️ Setup Instructions
1. PostgreSQL

Install PostgreSQL and create DB + user:

# Create user & database
sudo -u postgres psql -c "CREATE USER devuser WITH PASSWORD 'devpass';"
sudo -u postgres psql -c "CREATE DATABASE customers_fans OWNER devuser;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE customers_fans TO devuser;"


# 2. Environment Variables

Set for ETL & API:

export PGHOST=localhost
export PGPORT=5432
export PGUSER=devuser
export PGPASSWORD='devpass'
export PGDATABASE=customers_fans

(On Windows, use set instead of export.)

3. Create Schemas, Tables & Views

psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f sql/00_create_schemas_and_tables.sql
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f sql/01_mart_views.sql


4. Python Environment

python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r requirements.txt


5. Run ETL

# Fetch customers from API → landing → core
python etl/fetch_customers.py

# Load orders from XML → landing → core; rejects → core.load_rejects
python etl/load_orders.py


6. Run FastAPI

uvicorn api.main:app --reload --host 0.0.0.0 --port 8000


🔗 API Endpoints

Endpoint	Description
/health	Health check
/customers	List customers (supports page, page_size, country, q)
/customers/{customer_id}	Get customer profile + last 5 activities
/fan-score/{customer_id}	Get fan score of a customer
/fanbase/countries	Get top N countries by fanbase (top query param)
/orders	List orders (filters: customer_id, from_ts, to_ts, activity_type)


7. Instructions for Reviewer:

Clone the repository.

Set PostgreSQL environment variables: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE.

Run SQL scripts in order: 00_create_schemas_and_tables.sql → 01_mart_views.sql.

8. Activate virtual environment and install dependencies:

python -m venv .venv
.venv\Scripts\activate       # Windows
pip install -r requirements.txt


9. Run ETL scripts:


python -m etl.fetch_customers
python -m etl/load_orders.py



Start FastAPI server:

uvicorn api.main:app --reload --host 0.0.0.0 --port 8000


Test API endpoints or run api_test_client.py.

