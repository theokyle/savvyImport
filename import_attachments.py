import requests
import os
import pandas as pd
from paths import ENGAGEMENT_PATHS
from pymongo import MongoClient, UpdateOne


def download_all_engagement_attachments(limit=None, dry_run=False, update=False):
    print("📥 Searching engagement files for attachments...")

    # ⚡ Only load Mongo activities once
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    activity_collection = db["activities"]

    print("🔍 Preloading activity metadata...")
    activities = {
        str(a["externalId"]): a
        for a in activity_collection.find({}, {"_id": 1, "externalId": 1, "process": 1, "contact": 1})
        if a.get("externalId")
    }
    print(f"   → Loaded {len(activities)} activities")

    attachments = 0
    file_objs_to_upsert = []

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
                file_obj = download_attachment(file_id, dry_run=dry_run, update=update)
                if not file_obj:
                    continue

                # Attach activity/process/contact metadata
                file_obj["activity"] = activity.get("_id")
                file_obj["process"] = activity.get("process")
                file_obj["contact"] = activity.get("contact")

                file_objs_to_upsert.append(file_obj)
                
    save_attachments_batch(file_objs_to_upsert, dry_run=dry_run)
    print(f"✔ Done. Processed {len(file_objs_to_upsert)} attachments.")


def download_attachment(file_id, save_dir="./hubspot", dry_run=False, update=False):
    """ Returns a file object (real or simulated when dry_run=True). """
    TOKEN = os.getenv("HUBSPOT_API_KEY")
    if not update:
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

    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        attachment = r.json()
        signed_url = attachment.get("url")

        if not signed_url:
            print(f"⚠ No signed URL for file {file_id}")
            return None

        # Download the actual file content
        if not update:
            try:
                file_data = requests.get(signed_url, allow_redirects=True)
                file_data.raise_for_status()
            except requests.RequestException as e:
                print(f"⚠ Failed to download file {file_id} from signed URL: {e}")
                return None

        filename = attachment.get("name", f"{file_id}")
        extension = attachment.get("extension")
        filepath = f"{save_dir}/{filename}-{file_id}.{extension}"

        if not update:
            with open(filepath, "wb") as f:
                f.write(file_data.content)

        print(f"Downloaded {filepath}")
        return {
            "name": filename,
            "path": filepath,
            "size": attachment.get("size"),
            "type": attachment.get("type", "DOCUMENT"),
            "source": "Hubspot",
            "externalId": file_id
        }

    except requests.RequestException as e:
        print(f"⚠ Failed to get signed URL for file {file_id}: {e}")
        return None


def save_attachments_batch(file_objs, dry_run=False):
    if not file_objs:
        return

    if dry_run:
        print(f"[DRY-RUN] Total files: {len(file_objs)}")
        return

    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    file_collection = db["files"]

    operations = []

    for file_doc in file_objs:
        operations.append(
            UpdateOne(
                {"externalId": file_doc["externalId"]},  # match key
                {"$set": file_doc},                     # update fields
                upsert=True                             # insert if not exists
            )
        )

    if operations:
        result = file_collection.bulk_write(operations, ordered=False)
        print(f"✔ Bulk upsert completed: {result.matched_count} matched, "
              f"{result.upserted_count} inserted")




