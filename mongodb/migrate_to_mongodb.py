from sqlalchemy import create_engine, MetaData, Table, select
from pymongo import MongoClient

USER = 'user'
PASSWORD = 'Password123'
HOST = 'localhost'
DBNAME = 'mysqldb'
mysql_engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DBNAME}')
metadata = MetaData()
metadata.reflect(bind=mysql_engine)
connection = mysql_engine.connect()

mongo_client = MongoClient(f'mongodb://{USER}:{PASSWORD}@{HOST}/mongo?authSource=admin&retryWrites=true&w=majority')
mongo_db = mongo_client['mongo_db']
users_collection = mongo_db['users']
categories_collection = mongo_db['categories']
products_collection = mongo_db['products']
events_collection = mongo_db['events']

def migrate_events(batch_size):
    category_table = Table('Categories', metadata, autoload_with=mysql_engine)
    product_table = Table('Products', metadata, autoload_with=mysql_engine)
    event_table = Table('Events', metadata, autoload_with=mysql_engine)

    with mysql_engine.connect() as connection:
        categories = {row[0]: row[1] for row in connection.execute(select(category_table)).fetchall()}
        products = {
            row[0]: {
                "product_id": row[0], 
                "category": categories.get(row[1], ''), 
                "brand": row[2], 
                "price": float(row[3])
            } for row in connection.execute(select(product_table)).fetchall()
        }

        events_inserted = 0
        offset = 0

        # Migrate events in batches
        while True:
            event_batch = connection.execute(
                select(event_table).limit(batch_size).offset(offset)
            ).fetchall()

            if not event_batch:
                break

            events_data = [
                {
                    "_id": row[0], 
                    "event_time": row[1], 
                    "event_type": row[2], 
                    "product": products[row[3]], 
                    "user_id": row[4], 
                    "user_session": row[5]
                } for row in event_batch
            ]

            events_collection.insert_many(events_data)
            events_inserted += len(event_batch)
            offset += batch_size

    print(f"Migrated Events: {events_inserted}")

if __name__ == '__main__':
    migrate_events(100000)
