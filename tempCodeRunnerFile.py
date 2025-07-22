import pandas as pd
import mysql.connector


# Load data
df = pd.read_csv("D:\Data Analytics & Power BI Course\_PYTHON\Live 24 - Customer Cohort Analysis & ETL with Python and Google BigQuery\Online_Retail.csv")

# Connect to MySQL
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="test",
    password="test",
    database="online_retail_analysis"
)
cursor = conn.cursor()

# Define the SQL query
sql = """
INSERT INTO Online_Retail (
    InvoiceNo, StockCode, Descrptn, Quantity,
    InvoiceDate, UnitPrice, CustomerID, Country
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Convert DataFrame to list of tuples
data = [tuple(row) for row in df.itertuples(index=False)]

# Use executemany for bulk insert
cursor.executemany(sql, data)

# Commit and close
conn.commit()
cursor.close()
conn.close()

print("Bulk insert completed successfully!")