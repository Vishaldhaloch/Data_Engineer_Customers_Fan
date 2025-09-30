import re
import phonenumbers

EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')

def normalize_email(email):
    if not email:
        return None
    e = email.strip().lower()
    return e if EMAIL_RE.match(e) else None

def normalize_name(s):
    if not s:
        return None
    return s.strip().title()

def normalize_country(country_raw):
    if not country_raw:
        return None
    m = country_raw.strip()
    if m.upper() in ('USA','U.S.A','U.S.'):
        return 'United States'
    return m

def phone_to_e164(raw_phone, country=None):
    if not raw_phone:
        return None
    try:
        parsed = phonenumbers.parse(raw_phone, None)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None
    return None
