import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models_dw import Base

parser = argparse.ArgumentParser(description='Manage DW')
parser.add_argument('--drop', action='store_true', help='Drop all tables before insertion')
parser.add_argument('--create', action='store_true', help='Create tables before insertion')
parser.add_argument('--host', default='localhost', help='Host of DW')

args = parser.parse_args()

USER = 'user'
PASSWORD = 'Password123'
HOST = args.host 
DBNAME = 'dw'
DATABASE_URL = f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DBNAME}'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

if args.drop:
    Base.metadata.drop_all(engine)
    print("All tables dropped.")

if args.create:
    Base.metadata.create_all(engine)
    print("All tables created.")
