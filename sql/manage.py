import argparse
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from models import *
import pandas as pd

def find_and_remove_nan(chunk, column_names):
    new_chunk = []
    corrupted_chunk = []
    for row in chunk:
        if any(pd.isna(row[column_name]) for column_name in column_names):
            corrupted_chunk.append(row)
        else:
            new_chunk.append(row)
    return new_chunk, corrupted_chunk

def find_and_remove_corrupted_rows(chunk, column_name, bad_references):
    new_chunk = []
    removed_chunk = []
    for row in chunk:
        if row[column_name] in map(lambda x: x[column_name], bad_references):
            removed_chunk.append(row)
        else:
            new_chunk.append(row)
    return new_chunk, removed_chunk

def remove_duplicates(chunk, column_name, visited):
    return [row for row in chunk if row[column_name] not in visited]
    
def insert_from_csv(csv_file_path, engine, chunksize=10000): 
    metadata = MetaData()
    metadata.reflect(bind=engine)
    connection = engine.connect()

    # Assuming the tables are already defined and match CSV structure
    user_table = Table('Users', metadata, autoload_with=engine)
    category_table = Table('Categories', metadata, autoload_with=engine)
    product_table = Table('Products', metadata, autoload_with=engine)
    event_table = Table('Events', metadata, autoload_with=engine)
    visited_users = set()
    visited_categories = set()
    visited_products = set()

    for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
        # Preprocess chunk if necessary (e.g., drop NaN, convert datetimes)
        chunk['event_time'] = pd.to_datetime(chunk['event_time'], format='%Y-%m-%d %H:%M:%S %Z')
        
        # Convert DataFrame to list of dictionaries for bulk insert
        users = chunk[['user_id']].drop_duplicates().to_dict('records')
        users, corrupted_users = find_and_remove_nan(users, ['user_id'])
        users = remove_duplicates(users, "user_id", visited_users)

        categories = chunk[['category_id', 'category_code']].drop_duplicates().to_dict('records')
        categories, corrupted_categories = find_and_remove_nan(categories, ['category_id', 'category_code'])
        categories = remove_duplicates(categories, "category_id", visited_categories)

        products = chunk[['product_id', 'category_id', 'brand', 'price']].drop_duplicates().to_dict('records')
        products, corrupted_products = find_and_remove_nan(products, ['product_id', 'category_id', 'brand', 'price'])
        products, removed_products = find_and_remove_corrupted_rows(products, 'category_id', corrupted_categories)
        corrupted_products.extend(removed_products)
        products = remove_duplicates(products, "product_id", visited_products)

        events = chunk[['event_time', 'event_type', 'product_id', 'user_id', 'user_session']].to_dict('records')
        events, corrupted_events = find_and_remove_nan(events, ['event_time', 'event_type', 'product_id', 'user_id', 'user_session'])
        events, removed_events = find_and_remove_corrupted_rows(events, 'product_id', corrupted_products)
        corrupted_events.extend(removed_events)
        events, removed_events = find_and_remove_corrupted_rows(events, 'user_id', corrupted_users)
        corrupted_events.extend(removed_events)

        try:
            # Perform bulk insert
            connection.execute(user_table.insert(), users)
            connection.execute(category_table.insert(), categories)
            connection.execute(product_table.insert(), products)
            connection.execute(event_table.insert(), events)
            connection.commit()
            for user in users:
                visited_users.add(user["user_id"])
            for category in categories:
                visited_categories.add(category["category_id"])
            for product in products:
                visited_products.add(product["product_id"])
            
        except Exception as e:
            print(f"Error inserting data: {e}")
            connection.rollback()  # Roll back the transaction on error
            continue

        print(f"Inserted chunk of size {chunksize} into database.")

    print("All data committed.")
    connection.close()


# Setup argparse for script options
parser = argparse.ArgumentParser(description='Manage database schema operations.')
parser.add_argument('--drop', action='store_true', help='Drop all tables before creating new ones')
parser.add_argument('--create', action='store_true', help='Create new tables if they do not exist')
parser.add_argument('--insert_csv', type=str, help='File path to CSV to insert into the database')
args = parser.parse_args()

# Database connection and session creation
USER = 'user'
PASSWORD = 'Password123'
HOST = '25.49.45.229'
DBNAME = 'mysqldb'
engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DBNAME}')
Session = sessionmaker(bind=engine)
session = Session()

if args.drop:
    print("Dropping all tables...")
    Base.metadata.drop_all(engine)
    print("All tables dropped.")

if args.create:
    print("Creating new tables...")
    Base.metadata.create_all(engine)
    print("All tables created.")

if args.insert_csv:
    print(f"Inserting data from CSV file: {args.insert_csv}")
    insert_from_csv(args.insert_csv, engine)
    print("Data inserted.")