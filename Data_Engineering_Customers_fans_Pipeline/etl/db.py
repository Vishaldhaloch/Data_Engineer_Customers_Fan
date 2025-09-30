import os
import psycopg2
from contextlib import contextmanager

conninfo = {
    'host': os.getenv('PGHOST','localhost'),
    'port': os.getenv('PGPORT','5432'),
    'user': os.getenv('PGUSER','postgres'),
    'password': os.getenv('PGPASSWORD',''),
    'dbname': os.getenv('PGDATABASE','customers_fans')
}

@contextmanager
def get_conn():
    conn = psycopg2.connect(**conninfo)
    try:
        yield conn
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()
