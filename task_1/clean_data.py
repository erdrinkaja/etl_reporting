import os

import pandas as pd

PICKLE_FOLDER = "pickles"


# --- Save the data in pickle format for quick inspection and backup ---
def save_pickle(df, source_file):
    """
    Saves the cleaned DataFrame to a .pkl file.

    Why pickle?
    - Stores the full DataFrame including all column types and structure.
    - Much faster to read/write compared to CSV or Excel.
    - Preserves data types exactly (no need to re-parse).
    - Compressed by default, takes less disk space.
    """
    os.makedirs(PICKLE_FOLDER, exist_ok=True)
    pickle_path = os.path.join(PICKLE_FOLDER, f"{os.path.basename(source_file)}.pkl")
    df.to_pickle(pickle_path)


# --- Transform the data ---
def clean_sales_data(df, csv_file, rates):
    """
    Cleans and transforms the raw sales DataFrame:
    - Converts types (sales_amount to numeric, order_date to datetime)
    - Saves raw parsed data to pickle for inspection/debugging
    - Drops rows with missing critical fields
    - Fills missing optional fields with default values
    - Removes duplicates
    - Converts sales_amount to USD using provided exchange rates
    """

    df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce")
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    save_pickle(df, csv_file)

    df = df.dropna(subset=["sales_amount", "order_date", "currency"]).copy()
    df["affiliate_name"] = df["affiliate_name"].fillna("Unknown Affiliate")
    df["category"] = df["category"].fillna("Uncategorized")

    df['order_date'] = df['order_date'].dt.strftime('%Y-%m-%d')
    rates['date'] = pd.to_datetime(rates['date']).dt.strftime('%Y-%m-%d')

    # Merge to get the rate for each (date, currency)
    df_merged = df.merge(rates, left_on=['order_date', 'currency'], right_on=['date', 'currency'], how='left')

    df_merged['sales_amount_usd'] = df_merged['sales_amount'] / df_merged['rate']
    df_merged.drop(columns=['date'], inplace=True)

    df_merged = df_merged.drop_duplicates()

    return df_merged
