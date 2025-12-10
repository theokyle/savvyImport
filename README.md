# 🧩 Savvy Import

A command-line tool for importing HubSpot export CSV data into MongoDB.  
Supports importing **Contacts**, **Processes**, **Companies**, **Cohorts**, **Attachments** and **Activity records** (Calls, Emails, Meetings, Notes, Tasks, Files), linking activities to contacts, companies, and deals/processes via join tables.

---

## 🚀 Features

- Import HubSpot **Contacts**, **Companies**, **Processes**, and **Cohorts** from CSV
- Import **Activities** (Calls, Emails, Meetings, Notes, Tasks)
- Automatically joins activity data with **Contacts**, **Companies**, and **Processes** using association CSVs
- Optional `--dry-run` mode for testing without writing to MongoDB
- Limit records processed using `--limit`
- Modular and extensible — add new data importers easily

---

## 📦 Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/your-username/savvyImport.git
   cd savvyImport
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate   
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file in the project root:

   ```bash
   MONGODB=mongodb://localhost:27017
   DB_NAME=savvyImport
   ```

   Adjust the values as needed for your MongoDB connection.

---

## 📁 Data Folder Structure

All import CSV files should be placed in a `data/` folder in the project directory:
```
savvyImport/
├── data/
│   ├── Contacts.csv
│   ├── Companies.csv
│   ├── Deal.csv
│   ├── DealCohortsAssociations.csv
│   ├── EngagementCall.csv
│   ├── EngagementEmail.csv
│   ├── EngagementMeeting.csv
│   ├── EngagementNote.csv
│   ├── EngagementTask.csv
│   ├── EngagementContactAssociations.csv
│   ├── EngagementCompanyAssociations.csv
│   └── EngagementDealAssociations.csv
├── import_contact.py
├── import_activity.py
├── import_cohort.py
├── import_company.py
├── import_process.py
├── paths.py
├── main.py
└── ...
```

## 🧰 Usage

### Import Contacts

Imports contacts from a HubSpot export file.

```bash
python main.py contact
```

**Dry-run example (no DB writes):**
```bash
python main.py contact --dry-run --limit 10
```

---

### Import Activities

Activities (Calls, Emails, Meetings, Notes) require **join files** identified in paths.py that associates each engagement with a contact `VId`.

**Example:**
```bash
python main.py activity
```
---

### Import Processes, Companies, and Cohorts

```
python main.py process
python main.py company
python main.py cohort
```

### Import Files and Download Attachments

```
python main.py attachment
```

## ⚙️ CLI Options

| Flag        | Description                       | Example                                               |
| ----------- | --------------------------------- | ----------------------------------------------------- |
| `command`   | Which import to run               | `contact`, `activity`, `process`, `company`, `cohort` |
| `--limit`   | Limit number of records processed | `--limit 10`                                          |
| `--dry-run` | Run without writing to MongoDB    | `--dry-run`                                           |


---

## 🧹 Example Workflow

1. Import all contacts:

```
python main.py contact
```

2. Import companies:
```
python main.py company
```

3. Import cohorts:
```
python main.py cohort
```

4. Import Contact/Cohort Associations:
```
python main.py contact-cohort
```

5. Import processes:
```
python main.py process
```

6. Import activities:
```
python main.py activity
```

7. Import and Download Attachments
```
python main.py attachment
```
---
