from sqlalchemy import create_engine, MetaData, Table, select
from pymongo import MongoClient
from datetime import datetime

USER = 'user'
PASSWORD = 'Password123'
HOST = 'localhost'
DBNAME = 'dw'
mysql_engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DBNAME}')
metadata = MetaData()
metadata.reflect(bind=mysql_engine)
connection = mysql_engine.connect()

mongo_client = MongoClient(f'mongodb://{USER}:{PASSWORD}@{HOST}/mongo?authSource=admin&retryWrites=true&w=majority')
mongo_db = mongo_client['dw']
sales_collection = mongo_db['Sales']

def migrate_events(batch_size):
	dim_categories_table = Table('DimCategories', metadata, autoload_with=mysql_engine)
	dim_products_table = Table('DimProducts', metadata, autoload_with=mysql_engine)
	fact_sales_table = Table('FactSales', metadata, autoload_with=mysql_engine)

	# Denormalize the three tables
	query = select(fact_sales_table, dim_products_table.c.brand, dim_categories_table.c.category_code) \
    .join(dim_products_table, fact_sales_table.c.product_id == dim_products_table.c.product_id) \
    .join(dim_categories_table, fact_sales_table.c.category_id == dim_categories_table.c.category_id)


	with mysql_engine.connect() as connection:
		result = connection.execute(query)

		batch = []	
		for i, row in enumerate(result):
			sales_document = {
				'date': datetime.combine(row[1], datetime.min.time()),
				'quantity': row[3],
				'unique_customers': row[4],
				'product': {
					'product_id': row[5],
					'brand': row[7],
					'unit_price': float(row[2])
				},
				'category': {
					'category_id': row[6],
					'category_code': row[8]
				}
			}
			batch.append(sales_document)
			if (i+1) % batch_size == 0:
				sales_collection.insert_many(batch)
				batch = [] 

		sales_collection.insert_many(batch)

	print(f"Successfully migrated {i+1} sales events to MongoDB.")

if __name__ == '__main__':
	migrate_events(100000)