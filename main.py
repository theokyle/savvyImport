import os
import re
import argparse
import pandas as pd
from pymongo import MongoClient, UpdateOne 
from dotenv import load_dotenv
from datetime import datetime

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

# --- Transform Rows ---
def transform_row(row):
    first = row.get("firstname", "").strip()
    last = row.get("lastname", "").strip()
    email = normalize_email(row.get("email"))
    if not email or not first or not last:
        return None  # Skip invalid

    location = ", ".join(filter(None, [row.get("city", "").strip(), row.get("state", "").strip()]))

    return {
        "firstName": first,
        "lastName": last,
        "displayName": f"{first} {last}".strip(),
        "email": email,
        "phone": normalize_phone(row.get("phone") or row.get("mobilephone")),
        "type": row.get("contact_type", "Student").strip() or "Student",
        "city": row.get("city", "").strip(),
        "state": row.get("state", "").strip(),
        "postalCode": row.get("zip", "").strip(),
        "country": row.get("country", "USA").strip() or "USA",
        "graduationDate": parse_date(row.get("graduation_date")),
        "graduationStatus": "Graduated" if normalize_bool(row.get("graduated_")) else "",
        "gender": row.get("gender", "").strip(),
        "ethnicity": row.get("ethnicity", "").strip(),
        "veteranStatus": normalize_bool(row.get("veteran_status")),
        "externalId": row.get("VId", "").strip(),
        "fundingProvider": row.get("grant_funding", "").strip(),
        "location": location,
        "active": True,
        "deleted": False,
        "confirmed": False,
        "timezone": "America/Chicago",
        "source": "HubSpot",
    }

# --- Prepare Bulk Updates ---
def main():
    load_dotenv()

    # CLI arguments
    parser = argparse.ArgumentParser(description="Import HubSpot contacts CSV into MongoDB")
    parser.add_argument("--file", type=str, default="hubspot_contacts.csv", help="Path to HubSpot CSV file")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of contacts processed")
    parser.add_argument("--dry-run", action="store_true", help="Simulate import without writing to MongoDB")
    args = parser.parse_args()
    
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    contacts = db["Contact"]
    
    csv_path = args.file
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    print(f"Loading CSV from {csv_path}...")
    df = pd.read_csv(csv_path, dtype=str).fillna("")

    if args.limit:
        df = df.head(args.limit)

    operations = []
    skipped_rows = 0

    for i, row in df.iterrows():
        doc = transform_row(row)
        if not doc:
            skipped_rows += 1
            print(f"⚠️ Skipped invalid row #{i+1}: missing name or email")
            continue
        operations.append(
            UpdateOne({"email": doc["email"]}, {"$set": doc}, upsert=True)
        )
    
    if args.dry_run:
        print(f"🧪 Dry run complete — {len(operations)} upserts prepared, {skipped_rows} skipped.")
    else:
        if operations:
            print(f"🚀 Importing {len(operations)} contacts to MongoDB...")
            result = contacts.bulk_write(operations)
            total = (result.upserted_count or 0) + (result.modified_count or 0)
            print(f"✅ {total} contacts inserted or updated successfully.")
            print(f"⚙️ Skipped: {skipped_rows}")
        else:
            print("⚠️ No valid contacts to import.")


if __name__ == "__main__":
    main()
