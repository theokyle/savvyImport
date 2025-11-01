import pandas as pd
import re

def normalize_email(email):
    if pd.isna(email):
        return None
    email = email.strip().lower()
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return email if re.match(pattern, email) else None

def normalize_phone(phone):
    if pd.isna(phone) or not str(phone).strip():
        return None
    digits = re.sub(r'\D', '', str(phone))
    return f"+{digits}" if digits else None

def parse_date(value):
    if pd.isna(value) or not str(value).strip():
        return None
    try:
        return pd.to_datetime(value, errors='coerce').to_pydatetime()
    except Exception:
        return None

def normalize_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ["true", "yes", "1", "y"]
    return False