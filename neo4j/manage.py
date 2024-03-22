import argparse
import decimal
from datetime import datetime

import mysql.connector
from connection import Neo4jConnection

def fetch_data_from_mysql(limit=None):
    cnx = mysql.connector.connect(user='user', password='Password123',
                                  host='127.0.0.1',
                                  database='mysqldb')
    cursor = cnx.cursor(dictionary=True)

    query = ("SELECT e.event_time, e.event_type, e.user_id, e.user_session, "
             "p.product_id, p.brand, p.price, c.category_id, c.category_code "
             "FROM Events e "
             "JOIN Products p ON e.product_id = p.product_id "
             "JOIN Categories c ON p.category_id = c.category_id")

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query)

    data = [row for row in cursor]

    cursor.close()
    cnx.close()

    return data

def convert_to_unix_time(ts):
    return int(ts.timestamp())


def process_chunk(data, neo4j_conn):
    for event in data:
        if isinstance(event['event_time'], datetime):
            event_time = event['event_time']
        else:
            event_time = datetime.strptime(event['event_time'], '%Y-%m-%d %H:%M:%S')


        price_str = str(event['price']) if isinstance(event['price'], decimal.Decimal) else event['price']

        parameters = {
            'date': event_time.strftime('%Y-%m-%d'),
            'event_type': event['event_type'],
            'user_id': event['user_id'],
            'user_session': event['user_session'],
            'category_id': event['category_id'],
            'category_name': event['category_code'],
            'product_id': event['product_id'],
            'brand_name': event['brand'],
            'price': price_str,
        }
        query = """
            MERGE (time:Time {date: $date})
            MERGE (user:User {user_id: $user_id})
            MERGE (session:Session {session_id: $user_session})
            MERGE (user)-[:INITIATED]->(session)
            MERGE (session)-[:OCCURRED_ON]->(time)
            MERGE (category:Category {category_id: $category_id, name: $category_name})
            MERGE (session)-[:VIEWED_CATEGORY]->(category)
            MERGE (product:Product {product_id: $product_id, price: $price})
            MERGE (category)-[:CONTAINS_PRODUCT]->(product)
            MERGE (brand:Brand {name: $brand_name})
            MERGE (product)-[:BRANDED_AS]->(brand)
            MERGE (session)-[:INTERACTED_WITH {type: $event_type}]->(product)

        """

        neo4j_conn.query(query, parameters)


def insert_events_neo4j(neo4j_conn):
    data = fetch_data_from_mysql()
    process_chunk(data, neo4j_conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate data to Neo4j database.")
    parser.add_argument("--uri", type=str, default="bolt://localhost:7687", help="Neo4j URI")
    parser.add_argument("--user", type=str, default="neo4j", help="Neo4j User")
    parser.add_argument("--password", default="Password123", type=str, help="Neo4j Password")
    parser.add_argument("--limit", default="100", type=int, help="Limit the number of rows fetched from MySQL for testing")

    args = parser.parse_args()

    neo4j_conn = Neo4jConnection(args.uri, args.user, args.password)

    try:
        limit = args.limit
        print(f"Inserting events from MySQL database with a limit of {limit if limit else 'all'} rows...")
        data = fetch_data_from_mysql(limit)
        process_chunk(data, neo4j_conn)
        print(f"Events data inserted.")
    finally:
        neo4j_conn.close()

# to see all data: MATCH (n) RETURN (n)
# to delete db: MATCH (n) DETACH DELETE n