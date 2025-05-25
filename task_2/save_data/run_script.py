import os
from dotenv import load_dotenv

import logging

from check_files import mark_as_processed, is_already_processed
from clean_data import clean_sales_data
from load_data import load_to_sqlite

from fetch_data import fetch_csv_data, fetch_exchange_rates

load_dotenv()

logging.basicConfig(
    filename="etl.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def run_etl(csv_file, db, conn_str):
    print("Running ETL...")
    logging.info("Starting ETL for file: %s", csv_file)
    try:
        if is_already_processed(csv_file):
            logging.info("File %s already processed. Skipping.", csv_file)
            print(f"File {csv_file} has already been processed. Skipping.")
            return
        df = fetch_csv_data(csv_file)
        rates = fetch_exchange_rates(df)
        df_clean = clean_sales_data(df, csv_file, rates)

        load_to_sqlite(df_clean, db, rates)

        mark_as_processed(csv_file)
        logging.info("ETL completed successfully for file: %s", csv_file)
        print("ETL completed successfully.")
    except Exception as e:
        logging.error("ETL failed for file: %s with error: %s", csv_file, str(e), exc_info=True)
        print(f"ETL failed: {e}")


if __name__ == "__main__":
    # Configuration: File paths and API URL
    csv_data = os.getenv("CSV_DATA")
    db_file = os.getenv("SQLITE_DB_PATH_TWO")
    # Example PostgreSQL connection (peer auth using OS user):
    conn_str = os.getenv("POSTGRES_URL")

    # Execute the ETL process
    run_etl(csv_data, db_file, conn_str)
