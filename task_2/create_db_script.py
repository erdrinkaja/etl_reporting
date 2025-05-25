import os
from dotenv import load_dotenv

import sqlite3

load_dotenv()

def create_schema_sqlite():
    sqlite_db = os.getenv("SQLITE_DB_PATH_TWO")
    conn = sqlite3.connect(sqlite_db)
    cur = conn.cursor()
    tables = {
        "exchange_rates": """
            CREATE TABLE exchange_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                currency TEXT NOT NULL,
                rate REAL NOT NULL,
                UNIQUE(date, currency)
            )
        """,
        "sales": """
            CREATE TABLE sales (
                order_id INT NOT NULL PRIMARY KEY,
                affiliate_name TEXT NOT NULL,
                category TEXT NOT NULL,
                sales_amount REAL NOT NULL,
                currency TEXT NOT NULL,
                order_date TEXT NOT NULL,
                exchange_rate_id INT,
                FOREIGN KEY (exchange_rate_id) REFERENCES exchange_rates(id)
            )
        """,
    }
    for table_name, create_stmt in tables.items():
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        exists = cur.fetchone()
        if not exists:
            cur.execute(create_stmt)

    # for aggregation and fast lookup
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_order_date ON sales(order_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_exchange_rate_id ON sales(exchange_rate_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_affiliate_name ON sales(affiliate_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_category ON sales(category);")

    # join by date+currency:
    cur.execute("CREATE INDEX IF NOT EXISTS idx_exchange_rates_date_currency ON exchange_rates(date, currency);")

    # join by id:
    cur.execute("CREATE INDEX IF NOT EXISTS idx_exchange_rates_id ON exchange_rates(id);")

    conn.commit()
    conn.close()

create_schema_sqlite()
