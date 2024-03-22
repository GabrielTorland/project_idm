import argparse
from datetime import datetime
import pandas as pd
from connection import Neo4jConnection

def convert_to_unix_time(time_str):
    # Assuming the format is '2019-11-01 00:00:00 UTC'
    # Remove ' UTC' and parse
    dt = datetime.strptime(time_str.replace(' UTC', ''), '%Y-%m-%d %H:%M:%S')
    return int(dt.timestamp())
def insert_data_neo4j(csv_file_path, neo4j_conn, nrows=1000):
    # Read only the first 'nrows' of the CSV file
    chunk = pd.read_csv(csv_file_path, nrows=nrows)
    for index, row in chunk.iterrows():
            event_time = convert_to_unix_time(row['event_time'])
            try:
                # This will need adjustment based on the actual data format for category_id
                category_id = int(float(row['category_id']))
            except ValueError:
                category_id = row['category_id']

            # In a single query we're creating all entities and relationships between them
            neo4j_conn.query("""
            MERGE (user:User {user_id: $user_id})
            ON CREATE SET user.user_session = $user_session
            MERGE (category:Category {category_id: $category_id})
            ON CREATE SET category.category_code = $category_code
            MERGE (product:Product {product_id: $product_id, brand: $brand})
            ON CREATE SET product.price = $price
            MERGE (product)-[:BELONGS_TO]->(category)
            MERGE (event:Event {event_time: $event_time, event_type: $event_type})
            MERGE (event)-[:INVOLVED_USER]->(user)
            MERGE (event)-[:INVOLVED_PRODUCT]->(product)
            """, parameters={
                'user_id': row['user_id'],
                'user_session': row['user_session'],
                'product_id': row['product_id'],
                'brand': row['brand'],
                'price': float(row['price']),
                'category_id': category_id,
                'category_code': row['category_code'],
                'event_time': event_time,
                'event_type': row['event_type']
            })


def insert_users_and_categories_neo4j(csv_file_path, neo4j_conn):
    import pandas as pd

    for chunk in pd.read_csv(csv_file_path, chunksize=80000000):
        users = chunk[['user_id']].drop_duplicates().dropna()
        categories = chunk[['category_id', 'category_code']].drop_duplicates(subset=['category_id']).dropna()

        for index, user in users.iterrows():
            cypher_query = "MERGE (u:User {user_id: $user_id})"
            parameters = {'user_id': user['user_id']}
            neo4j_conn.query(cypher_query, parameters)

        for index, category in categories.iterrows():
            cypher_query = """
            MERGE (c:Category {category_id: $category_id})
            ON CREATE SET c.category_code = $category_code
            """
            parameters = {'category_id': category['category_id'], 'category_code': category['category_code']}
            neo4j_conn.query(cypher_query, parameters)


def insert_products_neo4j(csv_file_path, neo4j_conn):
    import pandas as pd

    for chunk in pd.read_csv(csv_file_path, chunksize=100000):
        products = chunk.drop_duplicates(subset=['product_id']).dropna()

        for index, product in products.iterrows():
            cypher_query = """
            MERGE (p:Product {product_id: $product_id})
            ON CREATE SET p.brand = $brand, p.price = $price
            WITH p
            MATCH (c:Category {category_id: $category_id})
            MERGE (p)-[:BELONGS_TO]->(c)
            """
            parameters = {
                'product_id': product['product_id'],
                'brand': product['brand'],
                'price': float(product['price']),
                'category_id': product['category_id']
            }
            neo4j_conn.query(cypher_query, parameters)

def insert_events_neo4j(csv_file_path, neo4j_conn, nrows=None):
    import pandas as pd

    # Adjust to read a limited number of rows if nrows is specified
    if nrows:
        chunk = pd.read_csv(csv_file_path, nrows=nrows)
        process_chunk(chunk, neo4j_conn)
    else:
        for chunk in pd.read_csv(csv_file_path, chunksize=100000):
            process_chunk(chunk, neo4j_conn)
    import pandas as pd

    for chunk in pd.read_csv(csv_file_path, chunksize=100000):
        events = chunk.dropna()

        for index, event in events.iterrows():
            cypher_query = """
            MERGE (e:Event {event_id: $event_id})
            ON CREATE SET e.event_time = $event_time, e.event_type = $event_type, e.user_session = $user_session
            WITH e
            MATCH (u:User {user_id: $user_id}), (p:Product {product_id: $product_id})
            MERGE (e)-[:INVOLVES_USER]->(u)
            MERGE (e)-[:INVOLVES_PRODUCT]->(p)
            """
            parameters = {
                'event_id': event['event_id'],
                'event_time': event['event_time'],
                'event_type': event['event_type'],
                'user_session': event['user_session'],
                'user_id': event['user_id'],
                'product_id': event['product_id']
            }
            neo4j_conn.query(cypher_query, parameters)

def process_chunk(chunk, neo4j_conn):
    chunk['event_time'] = pd.to_datetime(chunk['event_time'], format='%Y-%m-%d %H:%M:%S %Z')
    events = chunk.dropna()
    for index, event in events.iterrows():
        cypher_query = """
        MERGE (e:Event {event_id: $event_id})
        ON CREATE SET e.event_time = $event_time, e.event_type = $event_type, e.user_session = $user_session
        WITH e
        MATCH (u:User {user_id: $user_id}), (p:Product {product_id: $product_id})
        MERGE (e)-[:INVOLVES_USER]->(u)
        MERGE (e)-[:INVOLVES_PRODUCT]->(p)
        """
        parameters = {
            'event_id': event['event_id'],
            'event_time': event['event_time'],
            'event_type': event['event_type'],
            'user_session': event['user_session'],
            'user_id': event['user_id'],
            'product_id': event['product_id']
        }
        neo4j_conn.query(cypher_query, parameters)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate data to Neo4j database.")
    parser.add_argument("--uri", type=str, default="neo4j://localhost:7687", help="Neo4j URI")
    parser.add_argument("--user", type=str, default="neo4j", help="Neo4j User")
    parser.add_argument("--password", type=str, help="Neo4j Password")
    parser.add_argument("--csv_path", type=str, default="2019-Nov.csv", help="File path to CSV to insert into the database")
    parser.add_argument("--insert_users_and_categories", action="store_true", help="Insert users and categories from CSV")
    parser.add_argument("--insert_products", action="store_true", help="Insert products from CSV")
    parser.add_argument("--insert_events", action="store_true", help="Insert events from CSV")
    args = parser.parse_args()

    neo4j_conn = Neo4jConnection(args.uri, args.user, args.password)

    test_rows = 1000 #for testing 1000 rows

    try:
        if args.insert_users_and_categories:
            print(f"Inserting users and categories from CSV file: {args.csv_path}")
            insert_users_and_categories_neo4j(args.csv_path, neo4j_conn)
            print("Users and categories data inserted.")

        if args.insert_products:
            print(f"Inserting products from CSV file: {args.csv_path}")
            insert_products_neo4j(args.csv_path, neo4j_conn)
            print("Products data inserted.")

        if args.insert_events:
            print(f"Inserting events from CSV file: {args.csv_path}")
            insert_events_neo4j(args.csv_path, neo4j_conn, nrows=test_rows)  # Pass the test_rows here
            print(f"Events data inserted. Only the first {test_rows} rows.")

    finally:
        neo4j_conn.close()
