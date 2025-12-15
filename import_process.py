import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
from constants import DEALSTAGE_TO_STAGE, TRACTION_LEVELS, OWNER_ID_TO_CONTACT_ID
from paths import PROCESS_CSV, PROCESS_JOIN_PATHS

def import_process(limit=None, dry_run=False):
    path = PROCESS_CSV
    join_files = PROCESS_JOIN_PATHS

    print(f"📥 Loading Processes from: {path}")

    # --- Load main CSV ---
    df = pd.read_csv(path, dtype=str).fillna("")
    join_cohort = join_files[1]

    print(f"🔗 Loading cohort associations: {join_cohort}")
    df_cohort_join = pd.read_csv(join_cohort, dtype=str).fillna("")

    if limit:
        df = df.head(limit)

    print(f"✅ Loaded {len(df)} process rows")

    # --- Mongo setup ---
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    process_collection = db["processes"]
    contact_collection = db["contacts"]
    cohort_collection = db["cohorts"]

    # --- Preload contacts and cohorts ---
    print("⚡ Preloading contacts and cohorts from database...")
    contacts = {str(c["externalId"]): c["_id"] for c in contact_collection.find({}, {"externalId": 1})}
    cohorts = {str(c["externalId"]): c["_id"] for c in cohort_collection.find({}, {"externalId": 1})}
    
    df_contact_join = pd.read_csv(join_files[0], dtype=str).fillna("")
    df_cohort_join = pd.read_csv(join_files[1], dtype=str).fillna("")

    contact_map = {row["DealId"]: row["VId"] for _, row in df_contact_join.iterrows()}
    cohort_map  = {row["DealId"]: row["CohortsId"] for _, row in df_cohort_join.iterrows()}

    operations = []
    skipped = 0

    for _, row in df.iterrows():
        process_name = row.get("dealname")
        if not process_name:
            print("Name missing, skipped")
            skipped += 1
            continue
        
        dealId = row.get("DealId")
        # Look up external IDs
        vid = contact_map.get(dealId)
        cohort_ext = cohort_map.get(dealId)

        # Look up Mongo IDs
        contact_id = contacts.get(vid)
        cohort_id = cohorts.get(cohort_ext)
        
        traction = row.get("traction", "").strip()
        if traction not in TRACTION_LEVELS:
            traction = "Unknown"

        dealstage = str(row.get("dealstage"))
        stage_obj = DEALSTAGE_TO_STAGE.get(dealstage) or []
        if stage_obj:
            stage_obj["_id"] = ObjectId()

        owner_id = row.get("hs_all_owner_ids", "").strip()
        assignedTo = OWNER_ID_TO_CONTACT_ID.get(owner_id)


        process_doc = {
            "name": process_name.strip(),
            "assignedTo": assignedTo,
            "contact": contact_id,
            "cohort": cohort_id,
            "traction": traction,
            "stages": [stage_obj] if stage_obj else [],
            "currentStage": stage_obj["_id"] if stage_obj else None,
            "externalId": dealId,
            "source": "hubspot",
            "reason": row.get("reason", "").strip() or None,
        }

        operations.append(
            UpdateOne(
                {"externalId": row.get("DealId")},  # or use externalId if you track it
                {"$set": process_doc},
                upsert=True,
            )
        )

    # --- Dry run ---
    if dry_run:
        print(f"🧪 Dry run: {len(operations)} operations, {skipped} skipped")
        return

    # --- Execute bulk write ---
    if operations:
        print(f"🚀 Writing {len(operations)} processes…")
        process_collection.bulk_write(operations)
        print(f"✅ Process import complete, skipped {skipped} rows")
    else:
        print("⚠️ No valid processes found.")
