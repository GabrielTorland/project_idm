import argparse
from config import get_mysql_connection, get_neo4j_connection
from graph_operations import fetch_data_from_mysql, preprocess_categories, process_chunk, create_indexes
import os

def main(limit):
    mysql_cnx = get_mysql_connection()
    neo4j_conn = get_neo4j_connection()

    try:
        create_indexes(neo4j_conn)
        data = fetch_data_from_mysql(mysql_cnx, limit)
        preprocess_categories(data, neo4j_conn)
        process_chunk(data, neo4j_conn)
        print("Sales data inserted successfully.")
    finally:
        mysql_cnx.close()
        neo4j_conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate data to Neo4j.")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    main(args.limit)


