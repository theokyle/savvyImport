import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from normalize import normalize_email, normalize_phone, parse_date, normalize_bool
from constants import OWNER_ID_TO_CONTACT_ID
from paths import CONTACT_CSV, PROCESS_CSV, PROCESS_JOIN_PATHS

def import_contact(limit=None, dry_run=False):
    print(f"📥 Loading contacts from {CONTACT_CSV}...")

    df_contacts = pd.read_csv(CONTACT_CSV, dtype=str).fillna("")
    df_deals = pd.read_csv(PROCESS_CSV, dtype=str).fillna("")
    df_assoc = pd.read_csv(PROCESS_JOIN_PATHS[0], dtype=str).fillna("")

    if limit:
        df_contacts = df_contacts.head(limit)

    # ----------------------------
    # Build DealId → funding map
    # ----------------------------
    print("⚡ Preloading deal funding info...")
    deal_funding = {
        row["DealId"]: {
            "fundingPartner": row.get("approved_funding_partner", "").strip() or None,
            "fundingStatus": row.get("funding_status", "").strip() or None,
        }
        for _, row in df_deals.iterrows()
        if row.get("DealId")
    }

    # ------------------------------------
    # Build VId → DealId association map
    # ------------------------------------
    print("⚡ Preloading contact ↔ deal associations...")
    contact_to_deal = {}

    for _, row in df_assoc.iterrows():
        vid = row.get("VId", "").strip()
        deal_id = row.get("DealId", "").strip()
        if vid and deal_id and vid not in contact_to_deal:
            contact_to_deal[vid] = deal_id  # first deal wins

    # ----------------------------
    # Mongo setup
    # ----------------------------
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    contacts_col = db["contacts"]

    operations = []
    skipped = 0
    funding_update = 0

    # ----------------------------
    # Transform + upsert
    # ----------------------------
    for _, row in df_contacts.iterrows():
        vid = row.get("VId", "").strip()

        deal_id = contact_to_deal.get(vid)
        funding = deal_funding.get(deal_id, {}) if deal_id else {}

        if funding:
            funding_update += 1

        doc = transform_row(row, funding)

        if not doc or not doc.get("email"):
            skipped += 1
            continue

        operations.append(
            UpdateOne(
                {"email": doc["email"]},
                {"$set": doc},
                upsert=True
            )
        )

    if dry_run:
        print(f"🧪 Dry run — {len(operations)} upserts prepared, {skipped} skipped, {funding_update} updated funding.")
        return

    if operations:
        print(f"🚀 Importing {len(operations)} contacts...")
        result = contacts_col.bulk_write(operations)
        total = (result.upserted_count or 0) + (result.modified_count or 0)
        print(f"✅ {total} contacts inserted/updated")
        print(f"⚙️ Skipped: {skipped}")
    else:
        print("⚠️ No valid contacts to import.")



def transform_row(row, funding):
    first = row.get("firstname", "").strip()
    last = row.get("lastname", "").strip()
    email = normalize_email(row.get("email"))
    if not email:
        return None
    
    hs_owner_id = row.get("hs_all_owner_ids", "").strip()
    owner = OWNER_ID_TO_CONTACT_ID.get(hs_owner_id)

    display_name = f"{first} {last}".strip() if first or last else email

    location = ", ".join(
        filter(None, [row.get("city", "").strip(), row.get("state", "").strip()])
    )

    return {
        "firstName": first,
        "lastName": last,
        "displayName": display_name,
        "email": email,
        "owner": owner,
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
        "fundingPartner": funding.get("fundingPartner"),
        "fundingStatus": funding.get("fundingStatus"),
        "location": location,
        "active": True,
        "deleted": False,
        "confirmed": False,
        "timezone": "America/Chicago",
        "source": "HubSpot",
    }
