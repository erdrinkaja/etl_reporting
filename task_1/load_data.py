import sqlite3

from sqlalchemy import create_engine
import pandas as pd

# --- Step 3: Load ---

def load_to_sqlite(df, db):
    """
    Saves the cleaned DataFrame to a SQLite database.

    Args:
        df (pd.DataFrame): The cleaned and transformed sales data.
        db (str): Path to the SQLite database file.

    Behavior:
        - Creates or replaces a table named 'sales'.
        - Drops existing data if table already exists (if_exists="replace").
        - Does not write the DataFrame index as a column.
    """
    conn = sqlite3.connect(db)
    df.to_sql("sales", conn, if_exists="replace", index=False)
    conn.close()


def load_to_postgres(df, conn_str, table_name="sales"):
    """
    Saves the cleaned DataFrame to a PostgreSQL database.

    Args:
        df (pd.DataFrame): The cleaned and transformed sales data.
        conn_str (str): SQLAlchemy-compatible PostgreSQL connection string.
                        Format: postgresql://user:password@host:port/dbname
        table_name (str): Name of the target table in PostgreSQL.

    Behavior:
        - Creates the table if it doesn't exist.
        - Replaces existing table data (if_exists="replace").
        - Automatically infers and maps data types.
    """
    engine = create_engine(conn_str)
    with engine.begin() as connection:
        df.to_sql(table_name, con=connection, if_exists="replace", index=False)

