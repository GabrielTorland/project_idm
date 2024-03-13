import argparse
from sqlalchemy import create_engine, MetaData, Table, select
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
    
def insert_users_and_categories(csv_file_path, engine, chunksize=100000): 
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
        users = chunk[['user_id']].drop_duplicates()
        users = users[~users['user_id'].isin(visited_users)]
        users = users.dropna(subset=['user_id']).to_dict('records')

        categories = chunk[['category_id', 'category_code']].drop_duplicates(subset=['category_id'])
        categories = categories[~categories['category_id'].isin(visited_categories)]
        categories = categories.dropna(subset=['category_id', 'category_code']).to_dict('records')

       #events = chunk[['event_time', 'event_type', 'product_id', 'user_id', 'user_session']].to_dict('records')
       #events, corrupted_events = find_and_remove_nan(events, ['event_time', 'event_type', 'product_id', 'user_id', 'user_session'])
       #events, removed_events = find_and_remove_corrupted_rows(events, 'product_id', corrupted_products)
       #corrupted_events.extend(removed_events)
       #events, removed_events = find_and_remove_corrupted_rows(events, 'user_id', corrupted_users)
       #corrupted_events.extend(removed_events)

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

    for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):

        # Convert DataFrame to list of dictionaries for bulk insert
        products = chunk[['product_id', 'category_id', 'brand', 'price']].drop_duplicates(subset=['product_id'])
        products = products[~products['product_id'].isin(visited_products)]
        products = products.dropna(subset=['product_id', 'category_id', 'brand', 'price']).to_dict('records')
        # Find all the unique categories in the chunk
        categories_exist = {product['category_id']: False for product in products}
        for category_id in categories_exist:
            categories_exist[category_id] = connection.execute(select(Category).where(Category.category_id == category_id)).rowcount > 0

        products = [product for product in products if categories_exist[product['category_id']]]

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
    visited_products = connection.execute(select(Product.product_id).distinct())
    visited_products = set([product[0] for product in visited_products])

    for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
        chunk['event_time'] = pd.to_datetime(chunk['event_time'], format='%Y-%m-%d %H:%M:%S %Z')

        # Convert DataFrame to list of dictionaries for bulk insert
        events = chunk[['event_time', 'event_type', 'product_id', 'user_id', 'user_session']]
        events = events.dropna(subset=['event_time', 'event_type', 'product_id', 'user_id', 'user_session']).to_dict('records')

        product_exist = {event['product_id']: False for event in events}

        for product_id in product_exist:
            product_exist[product_id] = str(product_id) in visited_products
        events = [event for event in events if product_exist[event['product_id']]]

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
HOST = '192.168.2.101'
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
    print(f"Inserting data from CSV file: {args.csv_path}")
    insert_users_and_categories(args.csv_path, engine)
    print("Data inserted.")

if args.insert_products:
    print(f"Inserting data from CSV file: {args.csv_path}")
    insert_products(args.csv_path, engine)
    print("Data inserted.")

insert_events(args.csv_path, engine)