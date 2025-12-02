import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from normalize import parse_date
import numpy as np
from paths import ENGAGEMENT_PATHS, ENGAGEMENT_JOIN_PATHS

def import_activity(limit=None, dry_run=False):
    """
    Import multiple engagement CSVs safely, aggregating contacts, deals, and companies per EngagementId,
    and including type-specific fields. Limit applies per engagement type.
    """
    print("📥 Loading engagement data…")

    # Load join tables and aggregate to lists to avoid duplicates
    join_paths = ENGAGEMENT_JOIN_PATHS

    df_contact_assoc = pd.read_csv(join_paths['contact'], dtype=str).fillna("")
    df_contact_assoc = df_contact_assoc.groupby("EngagementId")["VId"].agg(list).reset_index()

    df_deal_assoc = pd.read_csv(join_paths['deal'], dtype=str).fillna("")
    df_deal_assoc = df_deal_assoc.groupby("EngagementId")["DealId"].agg(list).reset_index()

    df_company_assoc = pd.read_csv(join_paths['company'], dtype=str).fillna("")
    df_company_assoc = df_company_assoc.groupby("EngagementId")["CompanyId"].agg(list).reset_index()

    # Mongo setup
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    contacts_collection = db["contacts"]
    companies_collection = db["companies"]
    processes_collection = db["processes"]
    activities_collection = db["activities"]

    # Preload DB objects
    contacts = {str(c["externalId"]): c["_id"] for c in contacts_collection.find({}, {"externalId": 1})}
    companies = {str(c["externalId"]): c["_id"] for c in companies_collection.find({}, {"externalId": 1})}
    processes = {str(p["externalId"]): p["_id"] for p in processes_collection.find({}, {"externalId": 1})}

    operations = []
    skipped_rows = 0

    # Process each engagement CSV individually
    for path in ENGAGEMENT_PATHS:
        print(f"📂 Processing {path}…")
        df_engagement = pd.read_csv(path, dtype=str).fillna("")

        # Merge with aggregated join tables
        df_merged = df_engagement.merge(df_contact_assoc, on="EngagementId", how="left") \
                                 .merge(df_deal_assoc, on="EngagementId", how="left") \
                                 .merge(df_company_assoc, on="EngagementId", how="left")

        # Apply per-type limit
        if limit:
            df_merged = df_merged.head(limit)

        for _, row in df_merged.iterrows():
            # Map external IDs to Mongo IDs
            contact_ids = map_ids(row.get("VId"), contacts)
            process_ids = map_ids(row.get("DealId"), processes)
            company_ids = map_ids(row.get("CompanyId"), companies)

            if not contact_ids and not process_ids and not company_ids:
                print(row.get("EngagementId"))
                skipped_rows += 1
                continue

            engagement_type = row.get('engagement_type', '').strip().lower()

            # Base document
            activity_doc = {
                "contact": contact_ids or None,
                "process": process_ids or None,
                "company": company_ids or None,
                "externalId": row.get("EngagementId"),
                "source": "HubSpot",
                "metadata": row.to_dict()
            }

            # Type-specific fields
            match engagement_type:
                case "call":
                    activity_doc.update({
                        "type": "call",
                        "author": row.get("hs_created_by_user_id", None),
                        "subject": row.get("hs_call_title", row.get("hs_call_summary", "")).strip(),
                        "content": row.get("hs_call_body", "").strip(),
                        "status": row.get("hs_call_status", "Completed").strip(),
                        "dueDate": parse_date(row.get("hs_createdate"))
                    })
                case "meeting":
                    activity_doc.update({
                        "type": "meeting",
                        "author": row.get("hs_created_by_user_id", None),
                        "subject": row.get("hs_meeting_title", "").strip(),
                        "content": row.get("hs_meeting_body", "").strip(),
                        "status": row.get("hs_meeting_outcome", "Scheduled").strip(),
                        "dueDate": parse_date(row.get("hs_meeting_start_time")),
                        "endDate": parse_date(row.get("hs_meeting_end_time"))
                    })
                case "email":
                    activity_doc.update({
                        "type": "email",
                        "author": row.get("hs_created_by_user_id", None),
                        "subject": row.get("hs_email_subject", "").strip(),
                        "content": row.get("hs_body_preview", "").strip(),
                        "status": row.get("hs_email_status", "Sent").strip(),
                        "dueDate": parse_date(row.get("hs_createdate"))
                    })
                case "note":
                    activity_doc.update({
                        "type": "note",
                        "author": row.get("hs_created_by_user_id", None),
                        "subject": None,
                        "content": row.get("hs_note_body", "").strip(),
                        "status": "Completed",
                        "dueDate": parse_date(row.get("hs_createdate"))
                    })
                case "task":
                    activity_doc.update({
                        "type": "task",
                        "author": row.get("hs_created_by_user_id", None),
                        "subject": row.get("hs_task_subject", "").strip(),
                        "content": row.get("hs_task_body", "").strip(),
                        "status": "Completed" if row.get("hs_task_is_completed") == "true" else "Pending",
                        "dueDate": parse_date(row.get("hs_start_date"))
                    })
                case _:
                    print(f"⚠️ Unknown engagement type '{engagement_type}', skipping.")
                    skipped_rows += 1
                    continue

            operations.append(UpdateOne(
                {"externalId": row.get("EngagementId")},
                {"$set": activity_doc},
                upsert=True
            ))

    # Write or dry run
    if dry_run:
        print(f"🧪 Dry run complete — {len(operations)} activities prepared, {skipped_rows} skipped.")
    else:
        if operations:
            print(f"🚀 Importing {len(operations)} activities to MongoDB...")
            activities_collection.bulk_write(operations)
            print(f"✅ Activities imported. Skipped: {skipped_rows}")
        else:
            print("⚠️ No valid activities found.")

def map_ids(csv_value, mapping):
    """
    csv_value: scalar, list, or pandas Series/array of values from CSV
    mapping: dict from externalId -> MongoDB _id
    Returns a list of MongoDB _ids (empty list if nothing found)
    """
    ids = []

    # Normalize to iterable
    if csv_value is None or (isinstance(csv_value, float) and pd.isna(csv_value)):
        return ids
    if isinstance(csv_value, (list, pd.Series, pd.Index, np.ndarray)):
        values = csv_value
    else:
        values = [csv_value]

    for v in values:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        try:
            key = str(int(v))
        except (ValueError, TypeError):
            key = str(v)
        obj_id = mapping.get(key)
        if obj_id:
            ids.append(obj_id)

    return ids