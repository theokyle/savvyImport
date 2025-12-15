from pymongo import MongoClient, UpdateOne
import os

def update_stages():
    client = MongoClient(os.getenv("MONGODB"))
    db = client[os.getenv("DB_NAME")]
    processes = db["processes"]

    operations = []

    for proc in processes.find({"stages.0": {"$exists": True}, "currentStage": {"$exists": True}}):
        stages = proc.get("stages")
        current_stage_val = proc.get("currentStage")

        if stages:
            operations.append(
                UpdateOne(
                    {"_id": proc["_id"]},
                    {"$set": {"currentStage": stages[0]["_id"]}}
                )
            )

    if operations:
        result = processes.bulk_write(operations)
        print(f"✅ Bulk update complete. Matched: {result.matched_count}, Modified: {result.modified_count}")
    else:
        print("⚠️ No processes needed updating.")