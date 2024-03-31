import argparse
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

def determine_price_range(price):
    if price is None:
        return 'unknown'
    elif price < 20:
        return '0-20'
    elif 20 <= price < 100:
        return '20-100'
    elif 100 <= price < 500:
        return '100-500'
    else:
        return '500+'

def convert_to_unix_time(ts):
    return int(ts.timestamp())


def process_chunk(data, neo4j_conn):
    last_session_product = {}
    last_session_behavior = {}

    for event in data:
        event_time = datetime.strptime(event['event_time'], '%Y-%m-%d %H:%M:%S') if not isinstance(event['event_time'], datetime) else event['event_time']
        year, month, day = event_time.strftime('%Y'), event_time.strftime('%m'), event_time.strftime('%Y-%m-%d')

        session_key = f"{event['user_session']}_{event['event_type']}_{event_time.strftime('%Y-%m-%d %H:%M:%S')}"
        last_session_key = last_session_product.get(event['user_session'], None)
        price_range = determine_price_range(event['price'])

        # Split category_code into a hierarchy
        category_hierarchy = event['category_code'].split('.') if event['category_code'] else ['unknown']

        parameters = {
            'year': year,
            'month': month,
            'day': day,
            'event_time': event_time.isoformat(),
            'event_type': event['event_type'],
            'user_id': str(event['user_id']),
            'user_session': str(event['user_session']),
            'category_id': str(event['category_id']),
            'category_hierarchy': category_hierarchy,
            'product_id': str(event['product_id']),
            'brand_name': event['brand'] or 'unknown',
            'price_range': price_range,
            'last_product_id': last_session_product.get(event['user_session'], ""),
            'session_key': session_key,
            'last_session_key': last_session_key
        }

        query = """
        MERGE (year:Year {year: $year})
        MERGE (month:Month {month: $month})-[:PART_OF]->(year)
        MERGE (day:Day {date: $day})-[:PART_OF]->(month)
        MERGE (user:User {user_id: $user_id})
        MERGE (session:Session {session_id: $user_session, user_id: $user_id, event_time: $event_time})
        MERGE (product:Product {product_id: $product_id})
        MERGE (brand:Brand {name: $brand_name})
        MERGE (user)-[:INITIATED]->(session)
        MERGE (session)-[:INTERACTED_WITH]->(product)
        MERGE (product)-[:BRANDED_AS]->(brand)
        MERGE (session)-[:OCCURRED_ON]->(day)

        // Handle the category hierarchy
        WITH product, $category_hierarchy AS hierarchy
        UNWIND range(0, size(hierarchy) - 1) AS i
        WITH product, hierarchy[i] AS category, i, hierarchy[i-1] AS parentCategoryName
        MERGE (c:Category {name: category})
        WITH product, c AS currentCategory, i, parentCategoryName
        WHERE i > 0
        MATCH (parentCategory:Category {name: parentCategoryName})
        MERGE (parentCategory)-[:PARENT_OF]->(currentCategory)
        WITH product, currentCategory, i
        ORDER BY i DESC
        LIMIT 1
        MERGE (product)-[:BELONGS_TO]->(currentCategory)

        // Continue with existing MERGE operations for EventCharacteristic, PriceRange, etc.
        MERGE (eventChar:EventCharacteristic {type: $event_type})
        MERGE (priceRange:PriceRange {range: $price_range})
        CREATE (behavior:Behavior {type: $event_type, timestamp: $event_time, session_key: $session_key})-[:RELATED_TO]->(product)
        MERGE (session)-[:GENERATED]->(behavior)
        WITH behavior, session, product
        OPTIONAL MATCH (lastBehavior:Behavior) WHERE lastBehavior.session_key = $last_session_key
        FOREACH (_ IN CASE WHEN lastBehavior IS NOT NULL THEN [1] ELSE [] END |
            MERGE (lastBehavior)-[:FOLLOWED_BY]->(behavior)
        )
        WITH behavior, product
        OPTIONAL MATCH (lastProduct:Product) WHERE lastProduct.product_id = $last_product_id AND lastProduct.product_id <> product.product_id
        FOREACH (_ IN CASE WHEN lastProduct IS NOT NULL THEN [1] ELSE [] END |
            MERGE (lastProduct)-[:NEXT]->(product)
        )
        MERGE (session)-[:DEMONSTRATES]->(eventChar)
        MERGE (product)-[:IN_PRICE_RANGE]->(priceRange)
        MERGE (brand)-[:SHARED_BRAND]->(eventChar)
        MERGE (priceRange)-[:SHARED_PRICE]->(eventChar)
        """

        neo4j_conn.query(query, parameters)
        last_session_product[event['user_session']] = event['product_id']
        last_session_behavior[event['user_session']] = session_key


def create_indexes(neo4j_conn):
    indexes = [
        "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.user_id)",
        "CREATE INDEX IF NOT EXISTS FOR (s:Session) ON (s.session_id)",
        "CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.product_id)",
        "CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.category_id)",
        "CREATE INDEX IF NOT EXISTS FOR (d:Day) ON (d.date)",
        # Indexes for shared characteristic nodes
        "CREATE INDEX IF NOT EXISTS FOR (e:EventCharacteristic) ON (e.type)",
        "CREATE INDEX IF NOT EXISTS FOR (b:Brand) ON (b.name)",
        "CREATE INDEX IF NOT EXISTS FOR (pr:PriceRange) ON (pr.range)"
    ]
    for index in indexes:
        neo4j_conn.query(index, {})


def insert_events_neo4j(neo4j_conn):
    data = fetch_data_from_mysql()
    process_chunk(data, neo4j_conn)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate data to Neo4j database.")
    parser.add_argument("--uri", type=str, default="bolt://localhost:7687", help="Neo4j URI")
    parser.add_argument("--user", type=str, default="neo4j", help="Neo4j User")
    parser.add_argument("--password", default="Password123", type=str, help="Neo4j Password")
    parser.add_argument("--limit", default="10000", type=int, help="Limit the number of rows fetched from MySQL for testing")

    args = parser.parse_args()
    neo4j_conn = Neo4jConnection(args.uri, args.user, args.password)
    create_indexes(neo4j_conn)

    try:
        limit = args.limit
        print(f"Inserting events from MySQL database with a limit of {limit if limit else 'all'} rows...")
        data = fetch_data_from_mysql(limit)
        process_chunk(data, neo4j_conn)
        print("Events data inserted successfully.")
    finally:
        neo4j_conn.close()

# to see all data: MATCH (n) RETURN (n)
# to delete db: MATCH (n) DETACH DELETE n
