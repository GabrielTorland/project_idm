import argparse
from config import get_mysql_connection, get_neo4j_connection
from db_operations import fetch_data_from_mysql, preprocess_categories, process_chunk, create_indexes

def main(uri, user, password, limit):
    mysql_cnx = get_mysql_connection()
    neo4j_conn = get_neo4j_connection(uri, user, password)

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
    parser = argparse.ArgumentParser(description="Migrate data to Neo4j database.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="Password123")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    main(args.uri, args.user, args.password, args.limit)
