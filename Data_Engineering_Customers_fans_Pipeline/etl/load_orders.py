import xml.etree.ElementTree as ET
import json
from dateutil import parser
from datetime import datetime, timezone
from etl.db import get_conn

VALID_ACTIVITIES = {'purchase','wishlist','review','like'}

def parse_order_element(el):
    def t(tag): return (el.find(tag).text.strip()) if el.find(tag) is not None and el.find(tag).text else None
    return {
        'order_id': t('order_id'),
        'customer_external_id': t('customer_external_id'),
        'order_ts': t('order_ts'),
        'activity_type': t('activity_type'),
        'channel': t('channel'),
        'items': t('items'),
        'total_amount': t('total_amount'),
        'currency': t('currency')
    }

def validate_and_insert(order):
    order_id = order['order_id']
    if not order_id:
        return ('missing_order_id', None)
    act = order['activity_type']
    if act not in VALID_ACTIVITIES:
        return ('invalid_activity_type', f"activity_type={act}")
    try:
        order_ts = parser.isoparse(order['order_ts'])
    except Exception:
        return ('invalid_order_ts', f"order_ts={order['order_ts']}")
    if order_ts > datetime.now(timezone.utc):
        return ('order_ts_in_future', order['order_ts'])
    try:
        items = int(order['items'] or 0)
    except:
        items = 0
    try:
        total_amount = float(order['total_amount'] or 0)
    except:
        total_amount = 0.0
    if act == 'purchase':
        if items < 1 or total_amount < 0:
            return ('purchase_invalid_amount_items', json.dumps({'items':items,'total_amount':total_amount}))
    else:
        if items != 0 or total_amount != 0:
            return ('non_purchase_has_amount_items', json.dumps({'items':items,'total_amount':total_amount}))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
INSERT INTO core.order_activity (order_id, customer_id, order_ts, activity_type, channel, items, total_amount, currency)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (order_id) DO NOTHING
""", (order_id, order['customer_external_id'], order_ts.isoformat(), act, order['channel'], items, round(total_amount,2), order['currency']))
    return (None, None)

def load_xml_file(path):
    with open(path,'r',encoding='utf8') as f:
        raw_xml = f.read()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO landing.orders_activity_raw (payload) VALUES (%s)", (raw_xml,))
    root = ET.fromstring(raw_xml)
    for el in root.findall('order'):
        order = parse_order_element(el)
        err, info = validate_and_insert(order)
        if err:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
INSERT INTO core.load_rejects (context, natural_key, reason, payload)
VALUES (%s,%s,%s,%s)
""", ('orders_xml', order.get('order_id'), err, json.dumps(order)))

if __name__ == "__main__":
    load_xml_file('orders_activity.xml')
