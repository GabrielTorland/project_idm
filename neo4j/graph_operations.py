from datetime import datetime, date
from decimal import Decimal

from tqdm import tqdm
import time

def fetch_data_from_mysql(cnx, limit=None):
    start_time = time.time()
    cursor = cnx.cursor(dictionary=True)
    query = ("""
             SELECT fs.date AS sale_time, fs.quantity, fs.unit_price, fs.unique_customers, 
                    dp.product_id, dp.brand, 
                    dc.category_id, dc.category_code
             FROM FactSales fs
             JOIN DimProducts dp ON fs.product_id = dp.product_id
             JOIN DimCategories dc ON fs.category_id = dc.category_id
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


def preprocess_categories(data, neo4j_conn):
    unique_categories = set()
    category_relationships = set()
    for sale in data:
        category_hierarchy = sale['category_code'].split('.') if sale['category_code'] else ['unknown']
        unique_categories.update(category_hierarchy)

        for level in range(1, len(category_hierarchy)):
            child_category = category_hierarchy[level]
            parent_category = category_hierarchy[level - 1]
            category_relationships.add((parent_category, child_category))

    for category in unique_categories:
        neo4j_conn.query(
            """
            MERGE (cat:Category {name: $category})
            """,
            {'category': category}
        )

    for parent_category, child_category in category_relationships:
        neo4j_conn.query(
            """
            MATCH (parentCat:Category {name: $parentCategory}), (childCat:Category {name: $childCategory})
            MERGE (parentCat)-[:HAS_SUBCATEGORY]->(childCat)
            """,
            {
                'parentCategory': parent_category,
                'childCategory': child_category
            }
        )


def process_chunk(data, neo4j_conn, chunk_size=10000):
    start_time = time.time()

    event_type = "sale"
    neo4j_conn.query("""
    MERGE (event:Event {event: $Event})
    """, {'Event': event_type})

    for i in tqdm(range(0, len(data), chunk_size), desc="Chunk processing"):
        chunk_data = data[i:i + chunk_size]

        for sale in chunk_data:
            unit_price = float(sale['unit_price']) if isinstance(sale['unit_price'], Decimal) else sale['unit_price']
            sale_date = sale['sale_time'] if isinstance(sale['sale_time'], date) else datetime.strptime(
                sale['sale_time'], "%Y-%m-%d").date().isoformat()
            category_hierarchy = sale['category_code'].split('.') if sale['category_code'] else ['unknown']
            leaf_category = category_hierarchy[-1]

            sale_data = {
                'event_type': event_type,
                'product_id': sale['product_id'],
                'brand_name': sale.get('brand', 'unknown'),
                'leaf_category': leaf_category,
                'quantity': sale['quantity'],
                'unit_price': unit_price,
                'unique_customers': sale.get('unique_customers', 0),
                'date': sale_date
            }

            neo4j_conn.query("""
            MERGE (product:Product {product_id: $product_id})
            MERGE (category:Category {name: $leaf_category})
            MERGE (product)-[:BELONGS_TO]->(category)
            MERGE (brand:Brand {name: $brand_name})
            MERGE (brand)-[:HAS_PRODUCT]->(product)
            WITH product
            MATCH (event:Event {event: $event_type})
            CREATE (product)-[r:MADE_SALE]->(event)
            SET r.date = $date, r.quantity = $quantity, r.unit_price = $unit_price, r.unique_customers = $unique_customers
            """, sale_data)

    end_time = time.time()
    print(f"Time to process chunk: {end_time - start_time} seconds")


def create_indexes(neo4j_conn):
    start_time = time.time()
    neo4j_conn.query("CREATE INDEX IF NOT EXISTS FOR (b:Brand) ON (b.name)")
    neo4j_conn.query("CREATE INDEX IF NOT EXISTS FOR ()-[r:MADE_SALE]-() ON (r.date)")

    end_time = time.time()
    print(f"Time to create indexes: {end_time - start_time} seconds")
