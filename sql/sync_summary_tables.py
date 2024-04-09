from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError

# Database connection details
DW_USER = 'user'
DW_PASSWORD = 'Password123'
DW_HOST = 'localhost'
DW_DBNAME = 'dw'

# Create engine and connect to the database
engine = create_engine(f'mysql+mysqlconnector://{DW_USER}:{DW_PASSWORD}@{DW_HOST}/{DW_DBNAME}')
connection = engine.connect()

# Define or reflect your tables
metadata = MetaData()
metadata.reflect(bind=engine)

# Define the summary table if it does not exist
if 'BrandSalesSummary' not in metadata.tables:
    brand_sales_summary = Table('BrandSalesSummary', metadata,
        Column('brand', String(255), primary_key=True),
        Column('units_sold', Integer),
        Column('average_unit_price', Float),
        Column('number_of_products', Integer)
    )
    metadata.create_all(engine)
else:
    brand_sales_summary = metadata.tables['BrandSalesSummary']

# SQL statement for aggregating and inserting/updating data
aggregation_query = text("""
    INSERT INTO BrandSalesSummary (brand, units_sold, average_unit_price, number_of_products)
    SELECT dp.brand, SUM(fs.quantity) AS units_sold, AVG(fs.unit_price) AS average_unit_price, COUNT(*) AS number_of_products
    FROM FactSales AS fs
    JOIN DimProducts AS dp ON dp.product_id = fs.product_id
    GROUP BY dp.brand
    ON DUPLICATE KEY UPDATE
        units_sold = VALUES(units_sold),
        average_unit_price = VALUES(average_unit_price),
        number_of_products = VALUES(number_of_products);
""")

try:
    # Execute the aggregation and insertion query
    connection.execute(aggregation_query)
    connection.commit()
    print("BrandSalesSummary table has been updated successfully.")
except SQLAlchemyError as e:
    print(f"An error occurred: {e}")
finally:
    connection.close()
