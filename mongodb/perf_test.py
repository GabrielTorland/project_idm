import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import time
import random
from pymongo import MongoClient

NQUERIES = 100
NBULK = 20

def LatencySingleSelect(collection):
    query = {"product.product_id": '100000025'}
    execution_times = []

    for _ in range(NQUERIES):
        start_time = time.time()
        result = collection.find_one(query)
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

    avg = sum(execution_times) / NQUERIES
    print(f"Single Select | Average execution time: {avg*1000} ms")

def LatencyAggregate(collection):
    pipeline = [
        {"$group": {"_id": "$date", "unique_customers_sum": {"$sum": "$unique_customers"}}}
    ]
    execution_times = []

    for _ in range(NQUERIES):
        start_time = time.time()
        result = list(collection.aggregate(pipeline))
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

    avg = sum(execution_times) / NQUERIES
    print(f"Aggregate | Average execution time: {avg*1000} ms")

def ThroughputBulkDelete(collection, start_date, end_date):
    
    query = {"date": {"$gte": start_date, "$lte": end_date}}

    start_time = time.time()
    result = collection.delete_many(query)
    end_time = time.time()
    execution_time = end_time - start_time
    return execution_time

def ThroughputBulkInsert(collection):
    cat = 2053013552326770905
    prods = [100000025, 100000056, 100000026, 100000081]
    n = 40

    inserts = []
        
    start_date = datetime(2024, 4, 1)
    end_date = 0
    for i in range(n):
        current_date = (start_date + timedelta(days=i))
    
        fNum = random.uniform(5, 20)
        fNum = round(fNum, 2)
        iQuantity = random.randint(0,1000)
        iUnique = random.randint(0,1000)

        # Construct MongoDB document
        document = {
            "date": current_date,
            "unit_price": fNum,
            "quantity": iQuantity,
            "unique_customers": iUnique,
            "product_id": random.choice(prods),
            "category_id": cat
        }

        inserts.append(document)
        end_date = current_date

    execution_times = []
    delete_times = []

    for _ in range(NBULK):
        start_time = time.time()
        result = collection.insert_many(inserts)
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

        delTime = ThroughputBulkDelete(collection, start_date, end_date)
        delete_times.append(delTime)

    avgInsert = sum(execution_times) / NBULK
    avgDelete = sum(delete_times) / NBULK
    print(f"Bulk Insert | Average execution time: {avgInsert*1000} ms")
    print(f"Bulk Delete | Average execution time: {avgDelete*1000} ms")

if __name__ == "__main__":

    USER = 'root'
    PASSWORD = 'Password123'
    HOST = 'localhost'
    DBNAME = 'dw'

    mongo_client = MongoClient(f'mongodb://user:{PASSWORD}@{HOST}/mongo?authSource=admin&retryWrites=true&w=majority')
    mongo_db = mongo_client['dw']
    sales_collection = mongo_db['Sales']

    LatencySingleSelect(sales_collection)
    LatencyAggregate(sales_collection)
    ThroughputBulkInsert(sales_collection)



