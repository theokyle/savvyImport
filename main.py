import os
import argparse
import pandas as pd
from pymongo import MongoClient, UpdateOne 
from dotenv import load_dotenv
from import_contact import import_contact
from import_activity import import_activity
from import_cohort import import_cohort

# --- Prepare Bulk Updates ---
def main():
    load_dotenv()

    # CLI arguments
    parser = argparse.ArgumentParser(description="Import HubSpot data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_shared_arguments(subparser):
        subparser.add_argument("--limit", type=int, default=None, help="Limit number of contacts processed")
        subparser.add_argument("--dry-run", action="store_true", help="Simulate import without writing to MongoDB")

    contact_parser = subparsers.add_parser("contacts", help="Import contacts")
    contact_parser.add_argument(
        "--path", 
        type=str, 
        default="./data/Contact.csv", 
        help="Path to CSV Import File")
    add_shared_arguments(contact_parser)

    activity_parser = subparsers.add_parser("activities", help="Import activities and join to contacts")
    activity_parser.add_argument("--type", type=str, required=True, help="Type of Activity to be imported")
    activity_parser.add_argument(
        "--path", 
        type=str,
        help="Path to CSV Import File")
    activity_parser.add_argument(
        "--join", 
        type=str, 
        default="./data/EngagementContactAssociations.csv", 
        help="Path to activity-contact join CSV file")
    add_shared_arguments(activity_parser)

    process_parser = subparsers.add_parser("process", help="Import Process/Deals")
    process_parser.add_argument("--path", type=str, help="Path to CSV Import File")
    process_parser.add_argument(
        "--join", 
        type=str, 
        required=True, 
        help="Path to activity-contact join CSV file")
    
    cohort_parser = subparsers.add_parser("cohort", help="Import cohorts")
    cohort_parser.add_argument(
        "--path", 
        type=str, 
        default="./data/Cohorts.csv", 
        help="Path to CSV Import File")
    add_shared_arguments(cohort_parser)

    args = parser.parse_args()

    match args.command:
        case "contacts":
            import_contact(args.path, args.limit, args.dry_run)
        case "activities":
            activity_type = args.type.capitalize()
            if not args.path:
                args.path = f"./data/Engagement{activity_type}.csv"
            import_activity(activity_type, args.path, args.join, args.limit, args.dry_run)
        # case "process":
        #     import_processes(args.path, args.join, args.limit, args.dry_run)
        case "cohort":
            import_cohort(args.path, args.limit, args.dry_run)


if __name__ == "__main__":
    main()
