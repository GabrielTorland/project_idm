from sqlalchemy import create_engine, MetaData, Table, select, Date
from sqlalchemy import func

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
DW_DBNAME = 'dw'
dw_engine = create_engine(f'mysql+mysqlconnector://{DW_USER}:{DW_PASSWORD}@{DW_HOST}/{DW_DBNAME}')
dw_metadata = MetaData()
dw_metadata.reflect(bind=dw_engine)
dw_connection = dw_engine.connect()

def sync_category_dim(batch_size):
    categories_table_odb = Table('Categories', metadata_odb, autoload_with=odb_engine)
    categories_table_dw = Table('DimCategories', dw_metadata, autoload_with=dw_engine)
    
    dw_connection.execute(categories_table_dw.delete())
    dw_connection.commit()

    offset = 0
    categories_synced = 0
    while True:
        categories_batch = odb_connection.execute(
            select(categories_table_odb).limit(batch_size).offset(offset)
        ).fetchall()

        if not categories_batch:
            break

        categories_data = [
            {"category_id": row[0], "category_code": row[1]} for row in categories_batch
        ]

        dw_connection.execute(categories_table_dw.insert(), categories_data)
        dw_connection.commit()

        offset += batch_size
        categories_synced += len(categories_batch)
    
    print(f"Synced {categories_synced} categories from Categories to DimCategories")

def sync_product_dim(batch_size):
    products_table_odb = Table('Products', metadata_odb, autoload_with=odb_engine)
    products_table_dw = Table('DimProducts', dw_metadata, autoload_with=dw_engine)

    dw_connection.execute(products_table_dw.delete())
    dw_connection.commit()

    offset = 0
    products_synced = 0
    while True:
        products_batch = odb_connection.execute(
            select(products_table_odb).limit(batch_size).offset(offset)
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
        dw_connection.commit()

        offset += batch_size
        products_synced += len(products_batch)
    
    print(f"Synced {products_synced} products from Products to DimProducts")

def sync_fact(batch_size):
    events_table_odb = Table('Events', metadata_odb, autoload_with=odb_engine)
    products_table_odb = Table('Products', metadata_odb, autoload_with=odb_engine)
    fact_table_dw = Table('FactSales', dw_metadata, autoload_with=dw_engine)

    dw_connection.execute(fact_table_dw.delete())
    dw_connection.commit()

    subquery = select(
        func.date(events_table_odb.c.event_time).label('date'),
        func.max(products_table_odb.c.price).label('unit_price'),
        func.count().label('quantity'),
        func.count(func.distinct(events_table_odb.c.user_id)).label('unique_customers'),
        events_table_odb.c.product_id,
        products_table_odb.c.category_id
    ).select_from(
        events_table_odb.join(
            products_table_odb,
            events_table_odb.c.product_id == products_table_odb.c.product_id
        )
    ).group_by(
        func.date(events_table_odb.c.event_time),
        events_table_odb.c.product_id
    ).subquery()


    sales_data = odb_connection.execute(
        select(
            subquery.c.date,
            subquery.c.unit_price,
            subquery.c.quantity,
            subquery.c.unique_customers,
            subquery.c.product_id,
            subquery.c.category_id
        )
    ).fetchall()

    sales_data = [
        {
            "date": row[0],
            "unit_price": float(row[1]),
            "quantity": row[2],
            "unique_customers": row[3],
            "product_id": row[4],
            "category_id": row[5]
        } for row in sales_data
    ]

    for i in range(0, len(sales_data), batch_size):
        batch = sales_data[i:i+batch_size]
        dw_connection.execute(fact_table_dw.insert(), batch)
        dw_connection.commit()

    print(f"Synced {len(sales_data)} sales from Events to FactSales")

    print(f"Synced {len(sales_data)} sales from Events to FactSales")



if __name__ == '__main__':
    sync_category_dim(batch_size=100000)
    sync_product_dim(batch_size=100000)
    sync_fact(1000)
    odb_connection.close()
    dw_connection.close()