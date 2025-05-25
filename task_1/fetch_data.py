import pandas as pd
import requests


# --- Extract data---
def fetch_csv_data(sales_file):
    """
    Loads sales data from a CSV file into a Pandas DataFrame.

    Args:
        sales_file (str): Path to the sales CSV file.

    Returns:
        pd.DataFrame: Raw sales data.
    """
    df = pd.read_csv(sales_file)
    return df


def fetch_exchange_rates(df):
    """
    For each unique date in df['order_date'], fetches exchange rates vs USD from Frankfurter API.
    Returns a DataFrame with columns: date, currency, rate.
    """
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    unique_dates = pd.Series(df["order_date"].dropna().unique())
    unique_dates = unique_dates.dt.strftime('%Y-%m-%d').sort_values()

    rate_records = []

    for date_str in unique_dates:
        api_url = f"https://api.frankfurter.app/{date_str}?from=USD"
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        for currency, rate in data["rates"].items():
            rate_records.append({
                "date": date_str,
                "currency": currency,
                "rate": rate
            })
        rate_records.append({
            "date": date_str,
            "currency": "USD",
            "rate": 1
        })

    return pd.DataFrame(rate_records)
