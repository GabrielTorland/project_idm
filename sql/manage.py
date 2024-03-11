import argparse
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from models import *
import pandas as pd

def insert_from_csv(csv_file_path, session):
    df = pd.read_csv(csv_file_path, nrows=10000)
    
    for index, row in df.iterrows():
        if row.isnull().any():
            print(f"Skipping row {index} due to NaN values: {[col for col, value in row.items() if pd.isnull(value)]}")
            continue
        
        user = User(user_id=row['user_id'])
        session.merge(user)

        category = Category(category_id=row['category_id'], category_code=row['category_code'])  # Adjust as necessary
        session.merge(category)

        product = Product(product_id=row['product_id'], category_id=row['category_id'], brand=row['brand'], price=row['price'])
        session.merge(product)

        event_date = pd.to_datetime(row['event_time'], errors='coerce', format='%Y-%m-%d %H:%M:%S %Z')
        event = Event(event_time=event_date, event_type=row['event_type'], product_id=row['product_id'], user_id=row['user_id'], user_session=row['user_session'])
        session.merge(event)

        print(f"Inserted row {index} into database.")

        if index % 100 == 0:  # Commit per 100 rows inserted to avoid locking the table for too long
            session.commit()
    
    session.commit()  # Final commit to save any remaining data


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
    insert_from_csv(args.insert_csv, session)
    print("Data inserted.") 