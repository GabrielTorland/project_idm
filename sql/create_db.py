import mysql.connector
from mysql.connector import Error

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as e:
        print(f"The error '{e}' occurred")
        exit()  # Exit the script if connection is not successful
    return connection

def create_db(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL database successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

# Connection to the MySQL Server
server_connection = create_server_connection("25.49.45.229", "user", "Password123")

# Ask user if they want to drop and recreate the database
user_input = input("Do you want to drop the existing 'mysqldb' database and recreate it? (yes/no): ")
if user_input.lower() == 'yes':
    drop_db_query = "DROP DATABASE IF EXISTS mysqldb;"
    execute_query(server_connection, drop_db_query)
    create_db_query = "CREATE DATABASE mysqldb;"
    create_db(server_connection, create_db_query)

# Connect to the newly created Database
connection = create_connection("25.49.45.229", "user", "Password123", "mysqldb")

# SQL schema queries
create_users_table = """
CREATE TABLE IF NOT EXISTS Users (
    user_id VARCHAR(255) PRIMARY KEY
    -- Add more user-related fields here
);
"""

create_categories_table = """
CREATE TABLE IF NOT EXISTS Categories (
    category_id VARCHAR(255) PRIMARY KEY,
    category_code VARCHAR(255) DEFAULT NULL
);
"""

create_products_table = """
CREATE TABLE IF NOT EXISTS Products (
    product_id VARCHAR(255) PRIMARY KEY,
    category_id VARCHAR(255),
    brand VARCHAR(255) DEFAULT NULL,
    price DECIMAL(10, 2),
    FOREIGN KEY (category_id) REFERENCES Categories(category_id)
);
"""

create_events_table = """
CREATE TABLE IF NOT EXISTS Events (
    event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    product_id VARCHAR(255),
    user_id VARCHAR(255),
    user_session VARCHAR(255),
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);
"""

# Execute the queries to create tables
execute_query(connection, create_users_table)
execute_query(connection, create_categories_table)
execute_query(connection, create_products_table)
execute_query(connection, create_events_table)
