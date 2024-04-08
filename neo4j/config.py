from mysql.connector import connect
from connection import Neo4jConnection

def get_mysql_connection():
    return connect(user='user', password='Password123', host='localhost', database='dw')

def get_neo4j_connection(uri, user, password):
    return Neo4jConnection(uri=uri, user=user, password=password)
