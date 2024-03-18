import argparse
import multiprocessing
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import sessionmaker
from models import *
import pandas as pd
    
def insert_users_and_categories(csv_file_path, engine, chunksize=80000000): 
    metadata = MetaData()
    metadata.reflect(bind=engine)
    connection = engine.connect()

    user_table = Table('Users', metadata, autoload_with=engine)
    category_table = Table('Categories', metadata, autoload_with=engine)
    visited_users = set()
    visited_categories = set()

    for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
        # Dropping rows with duplicate user_id
        users = chunk[['user_id']].drop_duplicates()
        # Removing users that have already been added to the database
        users = users[~users['user_id'].isin(visited_users)]
        # Dropping rows where user_id is nan, and converting the remaining rows to dictionries
        users = users.dropna().to_dict('records')

        # Dropping rows with duplicate category_id
        categories = chunk[['category_id', 'category_code']].drop_duplicates(subset=['category_id'])
        # Dropping categories that have already been added to the database
        categories = categories[~categories['category_id'].isin(visited_categories)]
        # Dropping rows where category_id or category_code is nan, and converting the remaining rows to dictionries
        categories = categories.dropna().to_dict('records')

        try:
            # Perform bulk insert
            if len(users) > 0 and len(categories) > 0:
                connection.execute(user_table.insert(), users)
                connection.execute(category_table.insert(), categories)
            elif len(users) > 0:
                connection.execute(user_table.insert(), users)
            elif len(categories) > 0:
                connection.execute(category_table.insert(), categories)
            else:
                continue
            connection.commit()
            for user in users:
                visited_users.add(user["user_id"])
            for category in categories:
                visited_categories.add(category["category_id"])
            
        except Exception as e:
            print(f"Error inserting data: {e}")
            connection.rollback()  # Roll back the transaction on error
            continue

        print(f"Inserted chunk of size {len(users)+len(categories)} into database.")

    print("All data committed.")
    connection.close()

def insert_products(csv_file_path, engine, chunksize=100000): 
    metadata = MetaData()
    metadata.reflect(bind=engine)
    connection = engine.connect()

    product_table = Table('Products', metadata, autoload_with=engine)
    visited_products = set()
    visited_categories = connection.execute(select(Category.category_id).distinct())
    visited_categories = set([int(category[0]) for category in visited_categories])

    for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
        # Drop rows with duplicate product_id
        products = chunk[['product_id', 'category_id', 'brand', 'price']].drop_duplicates(subset=['product_id'])

        # Remove products that have already been added to the database
        products = products[~products['product_id'].isin(visited_products)]

        # Drop products with categories that doesn't exist
        products = products[products['category_id'].isin(visited_categories)]

        # Drop rows that contain nan in either of the below attributes
        products = products.dropna().to_dict('records')

        try:
            # Perform bulk insert
            if len(products) > 0:
                connection.execute(product_table.insert(), products)
                connection.commit()
                for product in products:
                    visited_products.add(product["product_id"])
            else:
                continue
            
        except Exception as e:
            print(f"Error inserting data: {e}")
            connection.rollback()  # Roll back the transaction on error
            continue

        print(f"Inserted chunk of size {len(products)} into database.")

    connection.close()
def insert_events(csv_file_path, engine, chunksize=100000): 
    metadata = MetaData()
    metadata.reflect(bind=engine)
    connection = engine.connect()

    event_table = Table('Events', metadata, autoload_with=engine)
    # Create a set with all the products in the database
    visited_products = connection.execute(select(Product.product_id).distinct())
    visited_products = set([int(product[0]) for product in visited_products])

    for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
        chunk['event_time'] = pd.to_datetime(chunk['event_time'], format='%Y-%m-%d %H:%M:%S %Z')

        # Extract valid rows/events
        events = chunk[['event_time', 'event_type', 'product_id', 'user_id', 'user_session']]
        # Drop events with a product that doesn't exist in the database
        events = events[events["product_id"].isin(visited_products)]        
        events = events.dropna().to_dict('records')

        try:
            # Perform bulk insert
            if len(events) > 0:
                connection.execute(event_table.insert(), events)
                connection.commit()
            else:
                continue
            
        except Exception as e:
            print(f"Error inserting data: {e}")
            connection.rollback()  # Roll back the transaction on error
            continue

        print(f"Inserted chunk of size {len(events)} into database.")

    connection.close()

# Setup argparse for script options
parser = argparse.ArgumentParser(description='Manage database schema operations.')
parser.add_argument('--drop', action='store_true', help='Drop all tables before creating new ones')
parser.add_argument('--create', action='store_true', help='Create new tables if they do not exist')
parser.add_argument('--csv_path', type=str, default="2019-Nov.csv", help='File path to CSV to insert into the database')
parser.add_argument('--insert_users_and_categories', action='store_true', help='Insert users and categories from CSV')
parser.add_argument('--insert_products', action='store_true', help='Insert products from CSV')
parser.add_argument('--insert_events', action='store_true', help='Insert events from CSV')
args = parser.parse_args()

# Database connection and session creation
USER = 'user'
PASSWORD = 'Password123'
HOST = 'localhost'
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

if args.insert_users_and_categories:
    print(f"Inserting users and categories from CSV file: {args.csv_path}")
    insert_users_and_categories(args.csv_path, engine)
    print("Data inserted.")

if args.insert_products:
    print(f"Inserting products from CSV file: {args.csv_path}")
    insert_products(args.csv_path, engine)
    print("Data inserted.")

if args.insert_events:
    print(f"Inserting events from CSV file: {args.csv_path}")
    insert_events(args.csv_path, engine)
    print("Data inserted.")
