import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from datetime import datetime
from paths import CONTACT_COHORT_CSV

def import_contact_cohort_links(limit=None, dry_run=False):
    print("📥 Importing Contact ↔ Cohort associations...")

    # Load CSV
    df = pd.read_csv(CONTACT_COHORT_CSV, dtype=str).fillna("")

    if limit:
        df = df.head(limit)

    # Connect to Mongo
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]

    contacts_col = db["contacts"]
    cohorts_col = db["cohorts"]
    cc_col = db["cohortcontacts"]

    # Preload contacts + cohorts
    print("   → Preloading contacts...")
    contacts = {
        str(c["externalId"]): c  
        for c in contacts_col.find({}, {"externalId": 1, "graduationDate": 1})
    }

    print("   → Preloading cohorts...")
    cohorts = {
        str(c["externalId"]): c["_id"]
        for c in cohorts_col.find({}, {"externalId": 1})
    }

    ops = []
    missing_contacts = 0
    missing_cohorts = 0

    # Process rows into bulk operations
    for _, row in df.iterrows():
        vid = row["VId"].strip()
        cohort_external = row["CohortsId"].strip()
        role = row.get("LabelContactTocohorts", "").strip() or "Student"

        contact = contacts.get(vid)
        cohort_id = cohorts.get(cohort_external)

        if not contact:
            missing_contacts += 1
            print(f"⚠️  Missing contact externalId {vid}")
            continue

        if not cohort_id:
            missing_cohorts += 1
            print(f"⚠️  Missing cohort externalId {cohort_external}")
            continue

        # Optional graduation date
        completion_date = contact.get("graduationDate") or None

        update_doc = {
            "contact": contact["_id"],
            "cohort": cohort_id,
            "role": role,
            "status": "Graduated",
            "completionDate": completion_date,
        }

        if dry_run:
            print("DRY RUN → Would UPSERT:", update_doc)
            continue

        # Build bulk UPSERT operation
        ops.append(
            UpdateOne(
                {"contact": contact["_id"], "cohort": cohort_id},  # unique pair
                {"$set": update_doc},
                upsert=True
            )
        )

    # Execute bulk write
    if not dry_run and ops:
        print(f"🚀 Executing bulk write with {len(ops)} operations...")
        result = cc_col.bulk_write(ops, ordered=False)
        print("Bulk write result:", result.bulk_api_result)

    print("\n✅ Import complete!")
    print(f"Upserts attempted: {len(ops)}")
    print(f"Missing contacts: {missing_contacts}")
    print(f"Missing cohorts: {missing_cohorts}")