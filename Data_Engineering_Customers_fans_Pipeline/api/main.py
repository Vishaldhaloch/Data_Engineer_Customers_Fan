from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import os
import psycopg2
from contextlib import contextmanager
from psycopg2.extras import RealDictCursor
from datetime import datetime

app = FastAPI(title="Customers->Fans API")

# PostgreSQL connection info
conninfo = {
    'host': os.getenv('PGHOST', 'localhost'),
    'port': os.getenv('PGPORT', '5432'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD', ''),
    'dbname': os.getenv('PGDATABASE', 'customers_fans')
}

@contextmanager
def get_conn():
    conn = psycopg2.connect(**conninfo)
    try:
        yield conn
    finally:
        conn.close()

# ----- Pydantic Models -----
class CustomerOut(BaseModel):
    customer_id: str
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    phone_e164: Optional[str]
    country: Optional[str]
    state: Optional[str]
    city: Optional[str]
    postcode: Optional[str]
    dob: Optional[str]

class OrderOut(BaseModel):
    order_id: str
    customer_id: str
    order_ts: str
    activity_type: str
    channel: str
    items: int
    total_amount: float
    currency: str

class CustomerProfileOut(BaseModel):
    profile: CustomerOut
    last_5_activities: List[OrderOut]

# ----- Utility to convert DB row -----
def convert_customer_row(row: dict) -> dict:
    row_copy = dict(row)
    dob = row_copy.get("dob")
    if isinstance(dob, datetime):
        row_copy["dob"] = dob.isoformat()
    elif dob is None:
        row_copy["dob"] = None
    else:
        row_copy["dob"] = str(dob)
    return row_copy

def convert_order_row(row: dict) -> dict:
    row_copy = dict(row)
    order_ts = row_copy.get("order_ts")
    if isinstance(order_ts, datetime):
        row_copy["order_ts"] = order_ts.isoformat()
    elif order_ts is None:
        row_copy["order_ts"] = None
    else:
        row_copy["order_ts"] = str(order_ts)
    return row_copy

# ----- Health Check -----
@app.get("/health")
def health():
    return {"status": "ok"}

# ----- List Customers -----
@app.get("/customers", response_model=List[CustomerOut])
def list_customers(page: int = 1, page_size: int = 20, country: Optional[str] = None, q: Optional[str] = None):
    offset = (page - 1) * page_size
    where = []
    params = []
    if country:
        where.append("country ILIKE %s")
        params.append(f"%{country}%")
    if q:
        where.append("(first_name ILIKE %s OR last_name ILIKE %s OR email ILIKE %s)")
        qpat = f"%{q}%"
        params.extend([qpat, qpat, qpat])
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT customer_id, first_name, last_name, email, phone_e164, country, state, city, postcode, dob
        FROM core.customer
        {where_sql}
        ORDER BY customer_id
        LIMIT %s OFFSET %s
    """
    params.extend([page_size, offset])
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [convert_customer_row(r) for r in rows]

# ----- Customer Profile + Last 5 Activities -----
@app.get("/customers/{customer_id}", response_model=CustomerProfileOut)
def get_customer(customer_id: str):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT customer_id, first_name, last_name, email, phone_e164, country, state, city, postcode, dob
                FROM core.customer
                WHERE customer_id=%s
            """, (customer_id,))
            cust = cur.fetchone()
            if not cust:
                raise HTTPException(status_code=404, detail="customer not found")
            cust = convert_customer_row(cust)

            cur.execute("""
                SELECT order_id, customer_id, order_ts, activity_type, channel, items, total_amount, currency
                FROM core.order_activity
                WHERE customer_id=%s
                ORDER BY order_ts DESC
                LIMIT 5
            """, (customer_id,))
            activities = [convert_order_row(r) for r in cur.fetchall()]
    return {"profile": cust, "last_5_activities": activities}

# ----- Fan Score -----
@app.get("/fan-score/{customer_id}")
def get_fan_score(customer_id: str):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM mart.v_customer_fan_score WHERE customer_id=%s", (customer_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="fan score not found")
    return dict(row)

# ----- Fanbase by Country -----
@app.get("/fanbase/countries")
def get_fanbase_countries(top: int = Query(10, gt=0)):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT country, fans_count, avg_fan_score, median_fan_score
                FROM mart.v_fanbase_by_country
                ORDER BY avg_fan_score DESC, fans_count DESC
                LIMIT %s
            """, (top,))
            rows = cur.fetchall()
    return [dict(r) for r in rows]

# ----- Orders with Filters -----
@app.get("/orders", response_model=List[OrderOut])
def list_orders(customer_id: Optional[str] = None, from_ts: Optional[str] = None,
                to_ts: Optional[str] = None, activity_type: Optional[str] = None):
    where = []
    params = []
    if customer_id:
        where.append("customer_id = %s"); params.append(customer_id)
    if from_ts:
        where.append("order_ts >= %s"); params.append(from_ts)
    if to_ts:
        where.append("order_ts <= %s"); params.append(to_ts)
    if activity_type:
        where.append("activity_type = %s"); params.append(activity_type)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT order_id, customer_id, order_ts, activity_type, channel, items, total_amount, currency
        FROM core.order_activity
        {where_sql}
        ORDER BY order_ts DESC
        LIMIT 1000
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [convert_order_row(r) for r in rows]
