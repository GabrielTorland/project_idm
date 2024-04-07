import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from tqdm import tqdm
from models_dw import Base, DimUser, DimCategory, DimProduct, FactSales


def insert_categories(session, chunk):
    existing_category_codes = {category[0] for category in session.query(DimCategory.category_code).all()}

    categories_data = chunk[['category_id', 'category_code']].drop_duplicates().dropna()
    new_categories_data = categories_data[~categories_data['category_code'].isin(existing_category_codes)]

    category_objects = []
    for index, row in new_categories_data.iterrows():
        if row['category_code'] not in existing_category_codes:
            category_objects.append(
                DimCategory(category_id=row['category_id'], category_code=row['category_code'])
            )
            existing_category_codes.add(row['category_code'])

    session.bulk_save_objects(category_objects)
    session.commit()


def insert_products(session, chunk):
    existing_product_ids = {product.product_id for product in session.query(DimProduct.product_id).all()}
    existing_category_ids = {category.category_id for category in session.query(DimCategory.category_id).all()}

    sales_data = chunk[chunk['event_type'] == 'purchase'].dropna()
    unique_purchased_products = set(sales_data['product_id'].unique())

    products_data = chunk[['product_id', 'category_id', 'brand', 'price']].drop_duplicates().dropna()

    new_products_data = products_data[
        (~products_data['product_id'].isin(existing_product_ids)) &
        (products_data['product_id'].isin(unique_purchased_products)) &
        (products_data['category_id'].isin(existing_category_ids))
    ]

    product_objects = []
    for index, row in new_products_data.iterrows():
        if row['product_id'] not in existing_product_ids:
            product_objects.append(
                DimProduct(
                    product_id=row['product_id'],
                    category_id=row['category_id'],
                    brand=row['brand'],
                    unit_price=row['price']
                )
            )
            existing_product_ids.add(row['product_id'])

    session.bulk_save_objects(product_objects)
    session.commit()



def insert_users(session, chunk):
    existing_user_ids = {user[0] for user in session.query(DimUser.user_id).all()}
    sales_data = chunk[chunk['event_type'] == 'purchase'].dropna()
    users_with_purchases = set(sales_data['user_id'].unique())
    new_user_ids = users_with_purchases - existing_user_ids

    user_objects = [
        DimUser(user_id=user_id)
        for user_id in new_user_ids
    ]

    session.bulk_save_objects(user_objects)
    session.commit()



def insert_fact_sales(session, chunk):
    existing_product_ids = {id[0] for id in session.query(DimProduct.product_id).all()}
    existing_user_ids = {id[0] for id in session.query(DimUser.user_id).all()}

    sales_data = chunk[chunk['event_type'] == 'purchase'].dropna()
    valid_sales_data = [
        sale for sale in sales_data.to_dict(orient='records')
        if sale['product_id'] in existing_product_ids and sale['user_id'] in existing_user_ids
    ]

    fact_sales_objects = [
        FactSales(sale_time=pd.to_datetime(sale['event_time']), product_id=sale['product_id'],
                  user_id=sale['user_id'], quantity=1, unit_price=sale['price'])
        for sale in valid_sales_data
    ]

    session.bulk_save_objects(fact_sales_objects)
    session.commit()


def insert_data_from_csv(csv_file_path, session, rows_limit=None):
    total_rows_processed = 0
    for chunk in tqdm(pd.read_csv(csv_file_path, chunksize=1000000), desc="Chunks Progress"):
        if rows_limit is not None and total_rows_processed >= rows_limit:
            break

        chunk = chunk.convert_dtypes().astype({'user_id': str, 'category_id': str, 'product_id': str, 'price': float})
        insert_categories(session, chunk)
        insert_users(session, chunk)
        insert_products(session, chunk)
        insert_fact_sales(session, chunk)

        total_rows_processed += len(chunk)
        if rows_limit is not None and total_rows_processed >= rows_limit:
            break


parser = argparse.ArgumentParser(description='CSV to MySQL Database Ingestion Script')
parser.add_argument('--csv_path', type=str, help='Path to the CSV file')
parser.add_argument('--drop', action='store_true', help='Drop all tables before insertion')
parser.add_argument('--create', action='store_true', help='Create tables before insertion')
parser.add_argument('--rows_limit', type=int, default=None,
                    help='Limit the number of rows to import for testing. Use "None" for no limit.')

args = parser.parse_args()

USER = 'user'
PASSWORD = 'Password123'
HOST = 'localhost'
DBNAME = 'mysqldb'
DATABASE_URL = f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DBNAME}'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

if args.drop:
    Base.metadata.drop_all(engine)
    print("All tables dropped.")

if args.create:
    Base.metadata.create_all(engine)
    print("All tables created.")

with Session() as session:
    insert_data_from_csv(args.csv_path, session, args.rows_limit)
    session.commit()
