from neo4j import GraphDatabase
from datetime import datetime, timedelta
import time
import random

NQUERIES = 100
NBULK = 20

def LatencySingleSelect(driver):
    cypher_query = "MATCH (p:Product {product_id: '100000025'}) RETURN p"
    execution_times = []

    for _ in range(NQUERIES):
        start_time = time.time()
        with driver.session() as session:
            result = list(session.run(cypher_query))
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

    avg = sum(execution_times) / NQUERIES
    print(f"Single Select | Average execution time: {avg*1000} ms")

def LatencyAggregate(driver):
    cypher_query = """
    MATCH (product:Product)-[sale:MADE_SALE]->()
    WITH date(sale.date) AS SaleDate, SUM(sale.unique_customers) AS TotalUniqueCustomers
    ORDER BY SaleDate
    RETURN TotalUniqueCustomers, toString(SaleDate.month) + '-' + toString(SaleDate.day) AS date
    """
    execution_times = []

    for _ in range(NQUERIES):
        start_time = time.time()
        with driver.session() as session:
            result = list(session.run(cypher_query))
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

    avg = sum(execution_times) / NQUERIES
    print(f"Aggregate | Average execution time: {avg*1000} ms")

def ThroughputBulkDelete(driver):
    cypher_query = """
    MATCH (product:Product)-[r:MADE_SALE]->(event:Event {event: "sale"})
    WHERE r.date >= "2024-04-02"
    DELETE r
    """
    start_time = time.time()
    with driver.session() as session:
        result = session.run(cypher_query)
    end_time = time.time()
    execution_time = end_time - start_time
    return execution_time

def ThroughputBulkInsert(driver):
    cat = 2053013552326770905
    prods = [100000025, 100000056, 100000026, 100000081]
    n = 40

    inserts = []
        
    start_date = datetime(2024, 4, 2)
    for i in range(n):
        current_date = (start_date + timedelta(days=i)).date()
        
        fNum = round(random.uniform(5, 20), 2)
        iQuantity = random.randint(0, 1000)
        iUnique = random.randint(0, 1000)
        product_id = random.choice(prods)
            
        inserts.append({
            'date': current_date,
            'unit_price': fNum,
            'quantity': iQuantity,
            'unique_customers': iUnique,
            'product_id': product_id,
            'category_id': cat
        })

    cypher_query = """
            MATCH (b:Brand {name: $category_id})-[hp:HAS_PRODUCT]->(p:Product {product_id: $product_id}),
                (e:Event {event: 'sale'})
            CREATE (p)-[ms:MADE_SALE {date: $date, quantity: $quantity, unique_customers: $unique_customers, unit_price: $unit_price}]->(e)
            RETURN ms
            """

    execution_times = []
    delete_times = []

    for _ in range(NBULK):
        start_time = time.time()
        with driver.session() as session:
            for insert_data in inserts:
                session.run(cypher_query, **insert_data)
                    
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

        delTime = ThroughputBulkDelete(driver)
        delete_times.append(delTime)

    avgInsert = sum(execution_times) / NBULK
    avgDelete = sum(delete_times) / NBULK
    print(f"Bulk Insert | Average execution time: {avgInsert*1000} ms")
    print(f"Bulk Delete | Average execution time: {avgDelete*1000} ms")

if __name__ == "__main__":

    #neo4j_conn = get_neo4j_connection()

    # Neo4j connection parameters
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "Password123"

    # Create a Neo4j driver
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    LatencySingleSelect(neo4j_driver)
    LatencyAggregate(neo4j_driver)
    ThroughputBulkInsert(neo4j_driver)


