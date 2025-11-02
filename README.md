# 🧩 Savvy Import

A command-line tool for importing HubSpot export CSV data into MongoDB.  
Supports importing **Contacts** and **Activity records** (Calls, Emails, Meetings, Notes), and linking activities to contacts via join tables.

---

## 🚀 Features

- Import HubSpot **Contacts** from CSV
- Import **Activities** (Calls, Emails, Meetings, Notes)
- Automatically joins activity data with contact associations using a join CSV
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
│   ├── EngagementCall.csv
│   ├── EngagementEmail.csv
│   ├── EngagementNote.csv
│   ├── EngagementMeeting.csv
│   ├── EngagementContactAssociations.csv
│   └── merged_preview.csv   # (optional debug output)
├── import_contact.py
├── import_activity.py
├── main.py
└── ...
```

---

## 🧰 Usage

### Import Contacts

Imports contacts from a HubSpot export file.

```bash
python main.py contacts --path ./data/Contacts.csv
```

**Dry-run example (no DB writes):**
```bash
python main.py contacts --path ./data/Contacts.csv --dry-run --limit 10
```

---

### Import Activities

Activities (Calls, Emails, Meetings, Notes) require a **join file** that associates each engagement with a contact `VId`.

**Example:**
```bash
python main.py activities   --path ./data/EngagementEmail.csv   --join ./data/EngagementContactAssociations.csv
```

**Dry-run example:**
```bash
python main.py activities   --path ./data/EngagementCall.csv   --join ./data/EngagementContactAssociations.csv   --dry-run --limit 5
```

You can use this same command for:
- `EngagementCall.csv`
- `EngagementEmail.csv`
- `EngagementMeeting.csv`
- `EngagementNote.csv`


---

## ⚙️ CLI Options

| Flag | Description | Example |
|------|--------------|----------|
| `--path` | Path to main CSV file | `--path ./data/EngagementEmail.csv` |
| `--join` | Path to contact join CSV | `--join ./data/EngagementContactAssociations.csv` |
| `--limit` | Limit number of records processed | `--limit 10` |
| `--dry-run` | Run without writing to MongoDB | `--dry-run` |

---

## 🧹 Example Workflow

1. Import all contacts:
   ```bash
   python main.py contacts --path ./data/Contacts.csv
   ```

2. Test import of emails:
   ```bash
   python main.py activities --path ./data/EngagementEmail.csv --join ./data/EngagementContactAssociations.csv --dry-run --limit 10
   ```

3. If everything looks good, import the rest:
   ```bash
   python main.py activities --path ./data/EngagementEmail.csv --join ./data/EngagementContactAssociations.csv
   ```

---
