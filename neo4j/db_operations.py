from datetime import datetime
from tqdm import tqdm
import time

def fetch_data_from_mysql(cnx, limit=None):
    start_time = time.time()
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

    # Create or merge categories
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

        # batch operation data
        sales_operations_data = []
        for sale in chunk_data:
            # parse and process the category hierarchy
            category_hierarchy = sale['category_code'].split('.') if sale['category_code'] else ['unknown']
            leaf_category = category_hierarchy[-1] if category_hierarchy != ['unknown'] else 'unknown'

            sales_operations_data.append({
                'user_id': sale['user_id'],
                'product_id': sale['product_id'],
                'brand_name': sale.get('brand', 'unknown'),
                'sale_time': sale['sale_time'].isoformat() if isinstance(sale['sale_time'], datetime) else sale[
                    'sale_time'],
                'quantity': sale['quantity'],
                'unit_price': str(sale['unit_price']),
                'leaf_category': leaf_category,
            })

        #  batch operation with UNWIND for product insertion
        neo4j_conn.query("""
        UNWIND $sales_operations_data AS sale_data
        MERGE (user:User {user_id: sale_data.user_id})
        MERGE (brand:Brand {name: sale_data.brand_name})
        MERGE (leafCategory:Category {name: sale_data.leaf_category})
        MERGE (product:Product {product_id: sale_data.product_id})
            ON CREATE SET product.unit_price = sale_data.unit_price
        MERGE (product)-[:BRANDED_AS]->(brand)
        MERGE (product)-[:BELONGS_TO]->(leafCategory)
        MERGE (user)-[:PURCHASED {sale_time: sale_data.sale_time, quantity: sale_data.quantity, unit_price: sale_data.unit_price}]->(product)
        """, {'sales_operations_data': sales_operations_data})

    end_time = time.time()
    print(f"Time to process chunk: {end_time - start_time} seconds")


def create_indexes(neo4j_conn):
    #makes stuff faster
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