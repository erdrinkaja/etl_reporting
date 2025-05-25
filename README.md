
# ETL Reporting Project

This project contains an end-to-end ETL (Extract, Transform, Load) pipeline and reporting solution for sales data, split into three main tasks:

- **Task 1:** Basic ETL pipeline (cleaning and storing in SQLite)
- **Task 2:** Relational schema with referential integrity and optimized storage
- **Task 3 & 4:** Automated querying and report generation (CSV & PDF with visualization)


## 1. Installation

### 1.1. Clone the Repository

```bash
git clone https://github.com/erdrinkaja/etl_reporting.git
cd etl_reporting
````

### 1.2. Create a Virtual Environment and Activate It

```bash
python3 -m venv .venv
source .venv/bin/activate          # For Mac/Linux
# .venv\Scripts\activate           # For Windows
```

### 1.3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 2. Environment Variables

Create a `.env` file in the project root and set the following variables
(**update the paths to match your machine**):

```env
SQLITE_DB_PATH_ONE=/Users/your_path/etl_reporting/task_1/task_one_etl.db
SQLITE_DB_PATH_TWO=/Users/your_path/etl_reporting/task_2/save_data/task_two_etl.db
POSTGRES_URL=postgresql:///postgres
CSV_DATA=/Users/your_path/etl_reporting/task_1/test_data.csv
```

---

## 3. How to Run

### Task 1: Basic ETL

1. Put your `test_data.csv` file in the `task_1/` directory.
2. Configure your `.env` file with the correct path for `CSV_DATA` and `SQLITE_DB_PATH_ONE`.
3. Run the ETL script:

   ```bash
   python task_1/run_script.py
   ```

---

### Task 2: Advanced Schema ETL

1. Run the database schema creation script:

   ```bash
   python task_2/create_db_script.py
   ```

   This creates the optimized tables and indexes in the database specified by `SQLITE_DB_PATH_TWO`.

2. Load and process your data:

   ```bash
   python task_2/save_data/run_script.py
   ```

---

### Task 3 & 4: Automated Reporting

1. Go to the `task_3_and_4/` directory.
2. Ensure your databases and data are ready (from previous tasks).
3. Run the report generator:

   ```bash
   python task_3_and_4/report_generator.py
   ```

   The script will generate and save both CSV and PDF reports in the `task_3_and_4/reports/` folder.

---

## 4. Project Structure

See the screenshot for an example directory tree. Key files and folders:

* `task_1/` – Basic ETL scripts and input data
* `task_2/save_data/` – Relational DB schema, advanced ETL
* `task_3_and_4/` – Reporting scripts, outputs to `reports/`
* `.env` – Environment variable definitions
* `requirements.txt` – All required Python libraries

---

## 5. Notes

* Update all paths in your `.env` to absolute paths on your computer.
* If you use PostgreSQL instead of SQLite, update `POSTGRES_URL` in your `.env` and scripts.
* The code uses environment variables for flexibility and security.
* For troubleshooting, check the generated `etl.log` files.

---

**Happy ETL-ing and Reporting!** 🚀

```
