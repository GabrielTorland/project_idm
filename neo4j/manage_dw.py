import argparse
from datetime import datetime
import mysql.connector
from connection import Neo4jConnection
from tqdm import tqdm
import time

def fetch_data_from_mysql(limit=None):
    start_time = time.time()
    cnx = mysql.connector.connect(user='user', password='Password123',
                                  host='localhost', database='mysqldb')
    cursor = cnx.cursor(dictionary=True)
    query = ("""
             SELECT fs.sale_time, fs.quantity, fs.unit_price, 
                    p.product_id, p.brand, 
                    c.category_id, c.category_code, 
                    u.user_id 
             FROM fact_sales fs  # Updated table name
             JOIN dim_product p ON fs.product_id = p.product_id  # Updated table name
             JOIN dim_category c ON p.category_id = c.category_id  # Updated table name
             JOIN dim_user u ON fs.user_id = u.user_id  # Updated table name
             """)
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    data = [row for row in cursor]
    cursor.close()
    cnx.close()

    end_time = time.time()
    print(f"Time to fetch data: {end_time - start_time} seconds")

    return data


def process_chunk(data, neo4j_conn, chunk_size=100000):
    """Processes data chunks to insert into Neo4j."""
    start_time = time.time()

    for i in tqdm(range(0, len(data), chunk_size), desc="Processing chunks"):
        chunk = data[i:i + chunk_size]
        sales_to_process = []
        for sale in chunk:
            sale['sale_time'] = sale['sale_time'].isoformat() if isinstance(sale['sale_time'], datetime) else sale['sale_time']
            sale['unit_price'] = str(sale['unit_price'])
            categories = sale['category_code'].split('/') if sale['category_code'] else ['unknown']
            main_category, sub_category = categories[0], categories[-1] if len(categories) > 1 else categories[0]

            sales_to_process.append({
                'user_id': sale['user_id'],
                'product_id': sale['product_id'],
                'brand_name': sale.get('brand', 'unknown'),
                'sale_time': sale['sale_time'],
                'quantity': sale['quantity'],
                'unit_price': sale['unit_price'],
                'main_category': main_category,
                'sub_category': sub_category
            })

        if sales_to_process:
            query_start_time = time.time()
            neo4j_conn.query("""
            UNWIND $sales_to_process AS sale
            MERGE (user:User {user_id: sale.user_id})
            MERGE (brand:Brand {name: sale.brand_name})
            MERGE (mainCat:Category {name: sale.main_category})
            MERGE (subCat:Category {name: sale.sub_category})
            MERGE (product:Product {product_id: sale.product_id})
                ON CREATE SET product.unit_price = sale.unit_price
            MERGE (product)-[:BRANDED_AS]->(brand)
            MERGE (product)-[:BELONGS_TO]->(subCat)
            MERGE (subCat)-[:SUBCATEGORY_OF]->(mainCat)
            MERGE (user)-[:PURCHASED {sale_time: sale.sale_time, quantity: sale.quantity, unit_price: sale.unit_price}]->(product)

            // Adjusted section for category-brand relationship
            WITH sale, brand, mainCat, subCat
            MERGE (subCat)-[:CONTAINS_BRAND]->(brand)

            // Optionally connect brand to main category if distinct from sub-category
            WITH DISTINCT brand, mainCat, subCat
            WHERE NOT mainCat = subCat
            MERGE (mainCat)-[:CONTAINS_BRAND]->(brand)
            """, {'sales_to_process': sales_to_process})

            query_end_time = time.time()
            print(f"\nTime to execute query: {query_end_time - query_start_time} seconds")

    end_time = time.time()
    print(f"Time to process chunk: {end_time - start_time} seconds")




def create_indexes(neo4j_conn):
    start_time = time.time()
    indexes = [
        "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.user_id)",
        "CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.product_id)",
        "CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.name)",
        "CREATE INDEX IF NOT EXISTS FOR (b:Brand) ON (b.name)",
        "CREATE INDEX IF NOT EXISTS FOR (s:Sale) ON (s.sale_time)"
    ]
    for index in indexes:
        neo4j_conn.query(index, {})
    end_time = time.time()
    print(f"Time to create indexes: {end_time - start_time} seconds")

def insert_sales_neo4j(neo4j_conn, limit=None):
    data = fetch_data_from_mysql(limit)
    process_chunk(data, neo4j_conn)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate data to Neo4j database.")
    parser.add_argument("--uri", type=str, default="bolt://localhost:7687", help="Neo4j URI")
    parser.add_argument("--user", type=str, default="neo4j", help="Neo4j User")
    parser.add_argument("--password", type=str, default="Password123", help="Neo4j Password")
    parser.add_argument("--limit", type=int, default=100000,
                        help="Limit the number of rows fetched from MySQL for testing")

    args = parser.parse_args()

    neo4j_conn = Neo4jConnection(uri=args.uri, user=args.user, password=args.password)
    create_indexes(neo4j_conn)

    try:
        print("Inserting sales data from MySQL database...")
        insert_sales_neo4j(neo4j_conn, args.limit)
        print("Sales data inserted successfully.")
    finally:
        neo4j_conn.close()