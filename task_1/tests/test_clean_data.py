import pandas as pd

from task_1.clean_data import clean_sales_data
from task_1.fetch_data import fetch_exchange_rates


def test_clean_sales_data_fullfile(tmp_path):
    csv_file = "task_1/test_data.csv"
    df = pd.read_csv(csv_file)
    rates = fetch_exchange_rates(df)
    cleaned = clean_sales_data(df, csv_file, rates)

    # Should have 8 rows (see expected cleaned data above)
    assert len(cleaned) == 8
    assert "Bob White" in cleaned["affiliate_name"].values
    assert "Electronics" in cleaned["category"].values
    assert cleaned["sales_amount_usd"].notna().all()  # All USD conversions present

    expected_order_ids = {101, 105, 106, 107, 108, 109, 111, 112}
    assert set(cleaned["order_id"]) == expected_order_ids

    # Check a specific row: EUR conversion (row with order_id 101, sales_amount 150, EUR)
    row_101 = cleaned[cleaned["order_id"] == 101].iloc[0]
    date_101 = row_101["order_date"]
    currency_101 = row_101["currency"]
    amount_101 = row_101["sales_amount"]
    rate_101 = rates[(rates["date"] == date_101) & (rates["currency"] == currency_101)]["rate"].values[0]
    assert row_101["sales_amount_usd"] == amount_101 / rate_101

