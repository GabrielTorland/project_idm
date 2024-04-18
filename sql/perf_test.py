import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models_dw import Base
from datetime import datetime, timedelta
import time
import random

NQUERIES = 100
NBULK = 20

def LatencySingleSelect(session):
    sql_query = "SELECT * FROM dw.DimProducts WHERE product_id = 100000025"
    execution_times = []

    for _ in range(NQUERIES):
        start_time = time.time()
        result = session.execute(text(sql_query))
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

    avg = sum(execution_times) / NQUERIES
    print(f"Single Select | Average execution time: {avg*1000} ms")

def LatencyAggregate(session):
    sql_query = "SELECT fs.date, SUM(fs.unique_customers) FROM dw.FactSales AS fs GROUP BY fs.date;"
    execution_times = []

    for _ in range(NQUERIES):
        start_time = time.time()
        result = session.execute(text(sql_query))
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

    avg = sum(execution_times) / NQUERIES
    print(f"Aggregate | Average execution time: {avg*1000} ms")

def ThroughputBulkDelete(session, start_date, end_date):
    
    sql_query = f"DELETE FROM FactSales WHERE date >= '{start_date}' AND date <= '{end_date}';"

    start_time = time.time()
    result = session.execute(text(sql_query))
    end_time = time.time()
    execution_time = end_time - start_time
    return execution_time

def ThroughputBulkInsert(session):
    cat = 2053013552326770905
    prods = [100000025, 100000056, 100000026, 100000081]
    n = 40

    inserts = []
        
    start_date = datetime(2024, 4, 1)
    end_date = 0
    for i in range(n):
        current_date = (start_date + timedelta(days=i)).date()
    
        fNum = random.uniform(5, 20)
        fNum = round(fNum, 2)
        iQuantity = random.randint(0,1000)
        iUnique = random.randint(0,1000)

        inserts.append(f"('{current_date}', {fNum}, {iQuantity}, {iUnique}, {prods[random.randint(0, len(prods)-1)]}, {cat})")
        end_date = current_date

    sql_query = "INSERT INTO FactSales (date, unit_price, quantity, unique_customers, product_id, category_id) VALUES "
    sql_query += ",\n".join(inserts[:-1]) + ";" 

    execution_times = []
    delete_times = []

    for _ in range(NBULK):
        start_time = time.time()
        result = session.execute(text(sql_query))
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

        delTime = ThroughputBulkDelete(session, start_date, end_date)
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
    engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DBNAME}')
    Session = sessionmaker(bind=engine)
    session = Session()

    LatencySingleSelect(session)
    LatencyAggregate(session)
    ThroughputBulkInsert(session)

    session.close()



