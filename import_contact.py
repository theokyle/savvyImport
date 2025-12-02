import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from normalize import normalize_email, normalize_phone, parse_date, normalize_bool
from paths import CONTACT_CSV

def import_contact(limit = None, dry_run = False):
    if not os.path.exists(CONTACT_CSV):
        raise FileNotFoundError(f"CSV file not found: {CONTACT_CSV}")
    print(f"Loading CSV from {CONTACT_CSV}...")
    df = pd.read_csv(CONTACT_CSV, dtype=str).fillna("")

    if limit:
        df = df.head(limit)

    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    contacts = db["contacts"]

    operations = []
    skipped_rows = 0

    for i, row in df.iterrows():
        doc = transform_row(row)
        if not doc:
            skipped_rows += 1
            continue
        operations.append(
            UpdateOne({"email": doc["email"]}, {"$set": doc}, upsert=True)
        )
    
    if dry_run:
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


def transform_row(row):
    first = row.get("firstname", "").strip()
    last = row.get("lastname", "").strip()
    email = normalize_email(row.get("email"))
    if not first and not last:
        displayName = email
    else:
        displayName = f"{first} {last}".strip()

    location = ", ".join(filter(None, [row.get("city", "").strip(), row.get("state", "").strip()]))

    return {
        "firstName": first,
        "lastName": last,
        "displayName": displayName,
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