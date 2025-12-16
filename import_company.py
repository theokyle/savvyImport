import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from normalize import normalize_phone, parse_date
from paths import COMPANY_CSV, COMPANY_JOIN_PATHS
from constants import OWNER_ID_TO_CONTACT_ID

def import_company(limit=None, dry_run=False):
    path = COMPANY_CSV
    join = COMPANY_JOIN_PATHS
    print(f"📥 Loading Companies from: {path}")

    # Load main CSV
    df_company = pd.read_csv(path, dtype=str).fillna("")
    if limit:
        df_company = df_company.head(limit)

    # --- Mongo setup ---
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    company_collection = db["companies"]
    contact_collection = db["contacts"]

    # --- 🔥 Preload all contacts into memory (fast lookup) ---
    print("⚡ Preloading contacts from database...")
    contacts = {
        str(c["externalId"]): c["_id"]
        for c in contact_collection.find({}, {"externalId": 1})
        if c.get("externalId")  # Skip if externalId is missing or None
    }
    print(f"   → Loaded {len(contacts)} contacts")

    company_contact_map = {}
    if join:
        df_join = pd.read_csv(join, dtype=str).fillna("")
        for _, row in df_join.iterrows():
            company_id = str(row.get("CompanyId", "")).strip()
            vid = str(row.get("VId", "")).strip()
            if company_id and vid and vid in contacts:
                company_contact_map.setdefault(company_id, []).append(contacts[vid])

    operations = []
    skipped = 0

    for _, row in df_company.iterrows():
        company_id = str(row.get("CompanyId")).strip()

        if not company_id:
            skipped += 1
            continue

        # --- Lookup related contacts via join table ---
        contact_object_ids = []

        contact_object_ids = company_contact_map.get(company_id, [])

        hs_owner_id = row.get("hs_all_owner_ids", "").strip()
        assignedTo = OWNER_ID_TO_CONTACT_ID.get(hs_owner_id)

        # --- Basic fields ---
        name = row.get("name") or row.get("website")
        if not name:
            skipped += 1
            continue

        address = {
            "line1": row.get("address", ""),
            "line2": row.get("address2", ""),
            "city": row.get("city", ""),
            "state": row.get("state", ""),
            "zip": row.get("zip", ""),
            "country": row.get("country", ""),
        }

        # --- Build Company doc ---
        company_doc = {
            "name": name,
            "assignedTo": assignedTo,
            "website": row.get("website"),
            "industry": row.get("industry"),
            "description": row.get("description"),
            "numberOfEmployees": (
                int(row["numberofemployees"])
                if row.get("numberofemployees", "").isdigit()
                else None
                ),
            "address": address,
            "phone": normalize_phone(row.get("phone")),
            "timezone": row.get("timezone"),
            "contacts": contact_object_ids,    
            "externalId": company_id,
            "social": {
                    "linkedin": row.get("linkedin_company_page"),
                    "twitter": row.get("twitterhandle"),
                    "facebook": row.get("facebook_company_page"),
                },
            "metadata": {
                "companyId": company_id,
                "createdAt": parse_date(row.get("hs_createdate")),
                "updatedAt": parse_date(row.get("hs_lastmodifieddate")),
                "lifecycleStage": row.get("lifecyclestage"),
                "score": (
                    int(row["hubspotscore"])
                    if row.get("hubspotscore", "").isdigit()
                    else None
                ),
                "lastContacted": parse_date(row.get("notes_last_contacted")),
                "lastActivity": parse_date(row.get("notes_last_updated")),
                "source": row.get("hs_object_source"),
                "notes": {
                    "lastUpdated": parse_date(row.get("notes_last_updated")),
                    "nextActivityDate": parse_date(row.get("notes_next_activity_date")),
                },
                "analytics": {
                    "numPageViews": (
                        int(row["hs_analytics_num_page_views"])
                        if row.get("hs_analytics_num_page_views", "").isdigit()
                        else None
                    ),
                    "numVisits": (
                        int(row["hs_analytics_num_visits"])
                        if row.get("hs_analytics_num_visits", "").isdigit()
                        else None
                    ),
                    "latestSource": row.get("hs_analytics_latest_source"),
                    "latestSourceData1": row.get("hs_analytics_latest_source_data_1"),
                    "latestSourceData2": row.get("hs_analytics_latest_source_data_2"),
                },
            },
        }

        operations.append(
            UpdateOne(
                {"externalId": company_id},
                {"$set": company_doc},
                upsert=True,
            )
        )

    # --- Dry run ---
    if dry_run:
        print(f"🧪 Dry run: {len(operations)} operations, {skipped} skipped")
        return

    # --- Execute bulk write ---
    if operations:
        print(f"🚀 Writing {len(operations)} companies…")
        company_collection.bulk_write(operations)
        print(f"✅ Company import complete")
    else:
        print("⚠️ No valid companies found.")
