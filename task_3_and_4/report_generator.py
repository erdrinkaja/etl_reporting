import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet
from dotenv import load_dotenv

load_dotenv()

REPORT_FOLDER = "reports"
os.makedirs(REPORT_FOLDER, exist_ok=True)
DB_PATH = os.getenv("SQLITE_DB_PATH_TWO")

# 1. Connect to DB
conn = sqlite3.connect(DB_PATH)

# 2. Query: Join sales & exchange_rates to get all sales in USD
query_sales_usd = """
SELECT 
    s.*, 
    er.rate, 
    s.sales_amount / er.rate AS sales_amount_usd
FROM sales s
JOIN exchange_rates er 
    ON s.exchange_rate_id = er.id
"""

df_sales = pd.read_sql_query(query_sales_usd, conn)

# 3. Aggregate by affiliate & category
agg_aff_cat = (
    df_sales.groupby(['affiliate_name', 'category'])
            .agg(total_sales_usd=pd.NamedAgg(column='sales_amount_usd', aggfunc='sum'))
            .reset_index()
)
agg_aff_cat = agg_aff_cat.rename(columns={
    "affiliate_name": "Affiliate Name",
    "category": "Category",
    "total_sales_usd": "Total Sales (USD)"
})
agg_aff_cat.to_csv(f"{REPORT_FOLDER}/total_sales_by_affiliate_category.csv", index=False)
agg_aff_cat["Total Sales (USD)"] = agg_aff_cat["Total Sales (USD)"].round(2)

# 4. Monthly summary
df_sales['order_month'] = pd.to_datetime(df_sales['order_date']).dt.to_period('M')
monthly_summary = (
    df_sales.groupby('order_month')
        .agg(
            total_sales_usd=pd.NamedAgg(column='sales_amount_usd', aggfunc='sum'),
            order_count=pd.NamedAgg(column='id', aggfunc='count')
        )
        .reset_index()
)
monthly_summary = monthly_summary.rename(columns={
    "order_month": "Order Month",
    "total_sales_usd": "Total Sales (USD)",
    "order_count": "Order Count"
})
monthly_summary.to_csv(f"{REPORT_FOLDER}/monthly_sales_summary.csv", index=False)
monthly_summary['Order Month'] = monthly_summary['Order Month'].astype(str)
monthly_summary['Total Sales (USD)'] = pd.to_numeric(monthly_summary['Total Sales (USD)'], errors='coerce')
monthly_summary["Total Sales (USD)"] = monthly_summary["Total Sales (USD)"].round(2)

# 5. Generate trend chart (bar or line plot)
plt.figure(figsize=(8, 4))
sns.lineplot(data=monthly_summary, x='Order Month', y='Total Sales (USD)', marker="o")
plt.title("Monthly Total Sales (USD)")
plt.xlabel("Month")
plt.ylabel("Total Sales (USD)")
plt.tight_layout()
chart_path = f"{REPORT_FOLDER}/monthly_trend.png"
plt.savefig(chart_path)
plt.close()

# 6. Prepare PDF Report
def make_pdf_report(pdf_path, agg_aff_cat, monthly_summary, chart_path):
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    # Title & Summary
    elements.append(Paragraph("Sales Report", styles['Title']))
    elements.append(Spacer(1, 12))

    # Summary Stats
    total_sales = agg_aff_cat['Total Sales (USD)'].sum()
    num_orders = df_sales.shape[0]
    elements.append(Paragraph(f"<b>Total Sales (USD):</b> {total_sales:,.2f}", styles['Normal']))
    elements.append(Paragraph(f"<b>Order Count:</b> {num_orders}", styles['Normal']))
    elements.append(Spacer(1, 12))

    # Table: Aggregated Sales by Affiliate & Category
    elements.append(Paragraph("Sales by Affiliate and Category", styles['Heading2']))
    table_data = [agg_aff_cat.columns.tolist()] + agg_aff_cat.values.tolist()
    t = Table(table_data)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('TEXTCOLOR', (0,0), (-1,0), colors.black),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 16))

    # Chart
    elements.append(Paragraph("Monthly Sales Trend", styles['Heading2']))
    elements.append(Image(chart_path, width=400, height=200))
    elements.append(Spacer(1, 12))

    # Table: Monthly Summary
    elements.append(Paragraph("Monthly Sales Summary", styles['Heading2']))
    table_month = [monthly_summary.columns.tolist()] + monthly_summary.astype(str).values.tolist()
    t2 = Table(table_month)
    t2.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.lightblue),
        ('TEXTCOLOR', (0,0), (-1,0), colors.black),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
    ]))
    elements.append(t2)

    doc.build(elements)

pdf_path = f"{REPORT_FOLDER}/etl_report.pdf"
make_pdf_report(pdf_path, agg_aff_cat, monthly_summary, chart_path)

print(f"Reports saved in {REPORT_FOLDER}:")
print(f"  - total_sales_by_affiliate_category.csv")
print(f"  - monthly_sales_summary.csv")
print(f"  - etl_report.pdf")
