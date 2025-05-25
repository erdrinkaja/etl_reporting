import sqlite3
import logging

def load_to_sqlite(df, db, rates_df):
    """
    Loads cleaned sales data and exchange rates into the SQLite database.

    Args:
        df (pd.DataFrame): Cleaned sales DataFrame.
        db (str): Path to SQLite database.
        rates_df (pd.DataFrame): DataFrame of exchange rates (columns: date, currency, rate).
    """
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    # --- Bulk insert exchange rates (skip if already present)
    exchange_rate_values = [
        (row['date'], row['currency'], row['rate'])
        for _, row in rates_df.iterrows()
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO exchange_rates (date, currency, rate) VALUES (?, ?, ?)",
        exchange_rate_values
    )

    # --- Build exchange_rate_id map for fast lookup
    cur.execute("SELECT id, date, currency FROM exchange_rates")
    exchange_rate_map = { (row[1], row[2]): row[0] for row in cur.fetchall() }

    # --- Bulk insert sales
    sales_values = []
    for idx, row in df.iterrows():
        rate_key = (row['order_date'], row['currency'])
        exchange_rate_id = exchange_rate_map.get(rate_key)
        if not exchange_rate_id:
            logging.error(
                f"Missing exchange_rate for sale {row['order_id']} ({row['order_date']}, {row['currency']})"
            )
            continue
        sales_values.append((
            int(row['order_id']),
            str(row['affiliate_name']),
            str(row['category']),
            float(row['sales_amount']),
            str(row['currency']),
            str(row['order_date']),
            exchange_rate_id
        ))

    # Handle "already exists" gracefully
    try:
        cur.executemany("""
            INSERT OR IGNORE INTO sales (
                order_id, affiliate_name, category, sales_amount, currency, order_date, exchange_rate_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, sales_values)
    except sqlite3.IntegrityError as e:
        logging.error(f"Sales insert failed: {e}")

    conn.commit()
    conn.close()
