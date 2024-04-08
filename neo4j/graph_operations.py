from datetime import datetime, date
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

    for i in tqdm(range(0, len(data), chunk_size), desc="Chunk processing"):
        chunk_data = data[i:i + chunk_size]

        sales_operations_data = []
        for sale in chunk_data:
            sale_date = sale['sale_time'] if isinstance(sale['sale_time'], date) else datetime.strptime(
                sale['sale_time'], "%Y-%m-%d").date()

            year, month, day = sale_date.year, sale_date.month, sale_date.day
            leaf_category = sale.get('category_code', 'unknown').split('.')[-1] if sale.get(
                'category_code') else 'unknown'

            sales_operations_data.append({
                'year': year,
                'month': month,
                'day': day,
                'product_id': sale['product_id'],
                'brand_name': sale.get('brand', 'unknown'),
                'leaf_category': leaf_category,
                'quantity': sale['quantity'],
                'unit_price': str(sale['unit_price']),
                'unique_customers': sale.get('unique_customers', 0),
                'sale_time': sale_date.isoformat()
            })

        neo4j_conn.query("""
        UNWIND $sales_operations_data AS sale_data
        MERGE (year:Year {name: sale_data.year})
        MERGE (year)-[:HAS_MONTH]->(month:Month {name: sale_data.month})
        MERGE (month)-[:HAS_DAY]->(day:Day {name: sale_data.day})

        MERGE (brand:Brand {name: sale_data.brand_name})
        MERGE (category:Category {name: sale_data.leaf_category})
        MERGE (product:Product {product_id: sale_data.product_id})

        MERGE (brand)-[:CATEGORIZED_AS]->(category)

        MERGE (product)-[:BRANDED_AS]->(brand)
        MERGE (product)-[:BELONGS_TO]->(category)
        MERGE (product)-[:MADE_SALE {Quantity: sale_data.quantity, Unit_price: sale_data.unit_price, Unique_customers: sale_data.unique_customers}]->(day)
        """, {'sales_operations_data': sales_operations_data})

    end_time = time.time()
    print(f"Time to process chunk: {end_time - start_time} seconds")


def create_indexes(neo4j_conn):
    start_time = time.time()
    indexes = [
        "CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.product_id)",
        "CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.name)",
        "CREATE INDEX IF NOT EXISTS FOR (b:Brand) ON (b.name)",
        "CREATE INDEX IF NOT EXISTS FOR (s:Sale) ON (s.time)"
    ]
    for index in indexes:
        neo4j_conn.query(index, {})
    end_time = time.time()
    print(f"Time to create indexes: {end_time - start_time} seconds")