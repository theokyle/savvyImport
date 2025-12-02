import os
import pandas as pd
from pymongo import UpdateOne, MongoClient
from paths import COHORT_CSV


def detect_cohort_type(cohort):
    if not cohort:
        return "FullStack"  # default fallback

    if "full" in cohort.lower():
        return "FullStack"
    elif "data" in cohort.lower():
        return "DAP"
    elif "cyber" in cohort.lower():
        return "CyberSecurity"

    return "FullStack"  # fallback if none matched


def import_cohort(limit=None, dry_run=False):
    csv_path = COHORT_CSV
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    print(f"Loading CSV from {csv_path}...")
    df = pd.read_csv(csv_path, dtype=str).fillna("")

    if limit:
        df = df.head(limit)

    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    cohorts = db["cohorts"]

    operations = []
    skipped = 0

    for _, row in df.iterrows():
        # External ID from HubSpot export
        external_id = row.get("CohortsId")
        if not external_id:
            skipped += 1
            continue
        name = row.get("cohort_name_")
        cohort_type = detect_cohort_type(row.get("class"))
        start_date = pd.to_datetime(row.get("cohort_start_date"), errors="coerce")
        end_date = pd.to_datetime(row.get("cohort_end_date"), errors="coerce")
        active = pd.isna(end_date) or end_date > pd.Timestamp.now()

        # HubSpot metadata storage
        metadata = {
            "objectId": row.get("hs_object_id"),
            "createdAt": pd.to_datetime(row.get("hs_createdate"), errors="coerce"),
            "updatedAt": pd.to_datetime(row.get("hs_lastmodifieddate"), errors="coerce"),
            "ownerId": row.get("hubspot_owner_id"),
            "teamIds": row.get("hs_all_team_ids"),
            "raw": row.to_dict()
        }

        cohort_doc = {
            "externalId": external_id,
            "name": name,
            "type": cohort_type,
            "active": bool(active),
            "startDate": start_date,
            "metadata": metadata
        }

        operations.append(
            UpdateOne(
                {"externalId": external_id},
                {"$set": cohort_doc},
                upsert=True
            )
        )

    if dry_run:
        print(f"🧪 Dry run: would upsert {len(operations)} cohort records.")
        return

    if not operations:
        print("⚠️ No valid cohort records found.")
        return

    
    result = cohorts.bulk_write(operations)
    print(f"✅ Upserted {result.upserted_count}, modified {result.modified_count} cohort records.")
