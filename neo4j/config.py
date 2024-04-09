from dotenv import load_dotenv
import os
from mysql.connector import connect
from connection import Neo4jConnection
load_dotenv()

def get_mysql_connection():
    return connect(
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        host='localhost',
        database=os.getenv('MYSQL_DATABASE')
    )

def get_neo4j_connection():
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    return Neo4jConnection(uri=uri, user=user, password=password)
