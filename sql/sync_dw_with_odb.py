from sqlalchemy import create_engine, MetaData, Table, select
from datetime import datetime

ODB_USER = 'user'
ODB_PASSWORD = 'Password123'
ODB_HOST = 'localhost'
ODB_DBNAME = 'mysqldb'
odb_engine = create_engine(f'mysql+mysqlconnector://{ODB_USER}:{ODB_PASSWORD}@{ODB_HOST}/{ODB_DBNAME}')
metadata_odb = MetaData()
metadata_odb.reflect(bind=odb_engine)
odb_connection = odb_engine.connect()

DW_USER = 'user'
DW_PASSWORD = 'Password123'
DW_HOST = 'localhost'
DW_DBNAME = 'mysqldb'
dw_engine = create_engine(f'mysql+mysqlconnector://{DW_USER}:{DW_PASSWORD}@{DW_HOST}/{DW_DBNAME}')
dw_metadata = MetaData()
dw_metadata.reflect(bind=dw_engine)
dw_connection = dw_engine.connect()


def sync_user_dim(batch_size):
    users_table_odb = Table('Users', metadata_odb, autoload_with=odb_engine)
    users_table_dw = Table('DimUsers', dw_metadata, autoload_with=dw_engine)
    
    dw_connection.execute(users_table_dw.delete())

    while True:
        users_batch = odb_connection.execute(
            select(users_table_odb).limit(batch_size)
        ).fetchall()

        if not users_batch:
            break

        users_data = [
            {"user_id": row[0]} for row in users_batch
        ]

        dw_connection.execute(users_table_dw.insert(), users_data)
    
    print("Synced UsersDim")

def sync_category_dim(batch_size):
    categories_table_odb = Table('Categories', metadata_odb, autoload_with=odb_engine)
    categories_table_dw = Table('DimCategories', dw_metadata, autoload_with=dw_engine)
    
    dw_connection.execute(categories_table_dw.delete())

    while True:
        categories_batch = odb_connection.execute(
            select(categories_table_odb).limit(batch_size)
        ).fetchall()

        if not categories_batch:
            break

        categories_data = [
            {"category_id": row[0], "category_code": row[1]} for row in categories_batch
        ]

        dw_connection.execute(categories_table_dw.insert(), categories_data)
    
    print("Synced CategoriesDim")

def sync_product_dim(batch_size):
    products_table_odb = Table('Products', metadata_odb, autoload_with=odb_engine)
    products_table_dw = Table('DimProducts', dw_metadata, autoload_with=dw_engine)

    dw_connection.execute(products_table_dw.delete())

    while True:
        products_batch = odb_connection.execute(
            select(products_table_odb).limit(batch_size)
        ).fetchall()

        if not products_batch:
            break

        products_data = [
            {
                "product_id": row[0], 
                "brand": row[2], 
                "price": float(row[3])
            } for row in products_batch
        ]

        dw_connection.execute(products_table_dw.insert(), products_data)
    
    print("Synced ProductsDim")

def sync_fact(batch_size):
    events_table_odb = Table('Events', metadata_odb, autoload_with=odb_engine)
    fact_table_dw = Table('FactSales', dw_metadata, autoload_with=dw_engine)

    dw_connection.execute(fact_table_dw.delete())

    while True:
        events_batch = odb_connection.execute(
            select(events_table_odb).limit(batch_size)
        ).fetchall()

        if not events_batch:
            break

        sales_data = [
            {
                "fact_id": row[0], 
                "date": row[1].date(), 
                "unit_price": float(row[2]),  
                "product_id": row[3], 
                "user_id": row[4], 
            } for row in events_batch
        ]

        dw_connection.execute(events_table_dw.insert(), events_data)
    
    print("Synced EventsFact")


if __name__ == '__main__':
    migrate_events(100000)
