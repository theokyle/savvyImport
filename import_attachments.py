import requests
import os
import pandas as pd
from paths import ENGAGEMENT_PATHS
from pymongo import MongoClient
from collections import defaultdict


def download_all_engagement_attachments(limit=None, dry_run=False):
    print("📥 Searching engagement files for attachments...")

    # ⚡ Only load Mongo activities once
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    activity_collection = db["activities"]

    print("🔍 Preloading activity metadata...")
    activities = {
        str(a["externalId"]): a
        for a in activity_collection.find({}, {"externalId": 1, "process": 1, "contact": 1})
    }
    print(f"   → Loaded {len(activities)} activities")

    attachments = 0

    for path in ENGAGEMENT_PATHS:
        print(f"📂 Checking {path}…")

        df = pd.read_csv(path, dtype=str).fillna("")

        if "hs_attachment_ids" not in df.columns:
            print("   → No 'hs_attachment_ids' column — skipping.")
            continue

        if limit:
            df = df.head(limit)

        for _, row in df.iterrows():
            att_raw = row.get("hs_attachment_ids", "").strip()
            if not att_raw:
                continue

            attachment_ids = [a.strip() for a in att_raw.split(";") if a.strip()]

            # Extract HubSpot engagement ID from CSV
            engagement_external_id = row.get("EngagementId")
            if not engagement_external_id:
                print("⚠ No EngagementID in row — skipping")
                continue

            # Map to activity_id in Mongo
            activity = activities.get(engagement_external_id)
            if not activity:
                print(f"⚠ No Mongo activity found for externalId={engagement_external_id}")
                continue

            for file_id in attachment_ids:
                attachments += 1

                file_obj = download_attachment(file_id, dry_run=dry_run)

                save_attachment_to_db(file_obj, activity, dry_run)

    print(f"✔ Done. Processed {attachments} attachments.")


def download_attachment(file_id, save_dir="./attachments", dry_run=False):
    """ Returns a file object (real or simulated when dry_run=True). """
    TOKEN = os.getenv("HUBSPOT_API_KEY")
    os.makedirs(save_dir, exist_ok=True)

    if dry_run:
        # print(f"[DRY-RUN] Would download attachment {file_id}")
        return {
            "name": f"DRYRUN_{file_id}.pdf",
            "path": "/dev/null",
            "size": 0,
            "type": "DOCUMENT",
            "source": "Hubspot",
            "externalId": file_id,
        }

    # ---- Real download ----
    url = f"https://api.hubapi.com/files/v3/files/{file_id}/signed-url"
    headers = {"Authorization": f"Bearer {TOKEN}"}

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    attachment = r.json()
    signed_url = attachment.get("url")

    if not signed_url:
        print(f"No signed URL for file {file_id}")
        return None

    file_data = requests.get(signed_url, allow_redirects=True)
    file_data.raise_for_status()

    filename = attachment.get("name", f"{file_id}.pdf")
    filepath = os.path.join(save_dir, filename)

    with open(filepath, "wb") as f:
        f.write(file_data.content)

    print(f"Downloaded {filename}")
    return {
        "name": filename,
        "path": filepath,
        "size": attachment.get("size"),
        "type": attachment.get("type", "DOCUMENT"),
        "source": "Hubspot",
        "externalId": file_id
    }


def save_attachment_to_db(file_obj, activity, dry_run=False):
    if not file_obj:
        return

    file_doc = file_obj.copy()
    file_doc["activity"] = activity.get("_id")
    file_doc["process"] = activity.get("process")
    file_doc["contact"] = activity.get("contact")

    if dry_run:
        return

    # ---- Real insert ----
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    file_collection = db["files"]

    result = file_collection.insert_one(file_doc)
    print(f"Inserted file with _id={result.inserted_id}")
    return result.inserted_id




