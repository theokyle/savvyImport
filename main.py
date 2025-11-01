import os
import argparse
import pandas as pd
from pymongo import MongoClient, UpdateOne 
from dotenv import load_dotenv
from import_contact import import_contact
from import_activity import import_activity

# --- Prepare Bulk Updates ---
def main():
    load_dotenv()

    # CLI arguments
    parser = argparse.ArgumentParser(description="Import HubSpot data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_shared_arguments(subparser):
        subparser.add_argument("--path", type=str, help="Path to CSV Import File")
        subparser.add_argument("--limit", type=int, default=None, help="Limit number of contacts processed")
        subparser.add_argument("--dry-run", action="store_true", help="Simulate import without writing to MongoDB")

    contact_parser = subparsers.add_parser("contacts", help="Import contacts")
    add_shared_arguments(contact_parser)

    activity_parser = subparsers.add_parser("activities", help="Import activities and join to contacts")
    activity_parser.add_argument("--join", type=str, required
    =True, help="Path to activity-contact join CSV file")
    add_shared_arguments(activity_parser)

    args = parser.parse_args()

    match args.command:
        case "contacts":
            import_contact(args.path, args.limit, args.dry_run)
        case "activities":
            import_activity(args.path, args.join, args.limit, args.dry_run)


if __name__ == "__main__":
    main()
