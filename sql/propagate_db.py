import csv
import mysql.connector
from datetime import datetime

# Database connection parameters
db_config = {
    'user': 'user',
    'password': 'Password123',
    'host': '25.49.45.229',
    'database': 'mysqldb',
    'raise_on_warnings': True
}

# Path to your CSV file
csv_file_path = '2019-Nov.csv'

# Connect to the database
db_conn = mysql.connector.connect(**db_config)
cursor = db_conn.cursor()

# Function to insert data into each table
def insert_data(table_name, data):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(sql, list(data.values()))

with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Insert user data
        insert_data('Users', {'user_id': row['user_id']})
with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Insert category data
        insert_data('Categories', {'category_id': row['category_id'], 'category_code': row['category_code']})
with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Insert product data
        insert_data('Products', {
            'product_id': row['product_id'], 
            'category_id': row['category_id'], 
            'brand': row['brand'], 
            'price': row['price']
        })

        # Convert 'event_time' from 'YYYY-MM-DD HH:MM:SS UTC' to 'YYYY-MM-DD HH:MM:SS'
        try:
            event_time = datetime.strptime(row['event_time'], '%Y-%m-%d %H:%M:%S %Z').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as ve:
            print(f"Error converting date: {ve}")
            continue  # Skip this row or handle error appropriately

        # Insert event data
        insert_data('Events', {
            'event_time': event_time,  # Use the converted 'event_time'
            'event_type': row['event_type'], 
            'product_id': row['product_id'], 
            'user_id': row['user_id'], 
            'user_session': row['user_session']
        })

# Close the cursor and connection
cursor.close()
db_conn.close()

print("Data import process completed.")
