import os
import argparse
import pandas as pd
from pymongo import MongoClient, UpdateOne 
from dotenv import load_dotenv
from import_contact import import_contact
from import_activity import import_activity
from import_cohort import import_cohort
from import_company import import_company
from import_process import import_process

# --- Prepare Bulk Updates ---
def main():
    load_dotenv()

    # CLI arguments
    parser = argparse.ArgumentParser(description="Import HubSpot data into MongoDB")

    parser.add_argument(
        "command",
        type=str,
        help="Which import command to run (contact, activity, process, cohort, company)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without writing to the database"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of rows to import for testing"
    )

    args = parser.parse_args()

    match args.command:
        case "contact":
            import_contact(args.limit, args.dry_run)
        case "activity":
            import_activity(args.limit, args.dry_run)
        case "process":
            import_process(args.limit, args.dry_run)
        case "cohort":
            import_cohort(args.limit, args.dry_run)
        case "company":
            import_company(args.limit, args.dry_run)


if __name__ == "__main__":
    main()
