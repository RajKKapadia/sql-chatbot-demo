import mysql.connector
import json

import config

# Function to determine MySQL data type based on Python data type


def get_mysql_data_type(value):
    if isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, str):
        return "VARCHAR(255)"
    elif isinstance(value, list):
        return "TEXT"  # You might want to adjust this based on your specific needs
    elif isinstance(value, dict):
        return "TEXT"  # You might want to adjust this based on your specific needs
    else:
        return "TEXT"  # Fallback to TEXT for other data types


# Connect to MySQL database
connection = mysql.connector.connect(
    host=config.MYSQL_HOST,
    port=config.MYSQL_PORT,
    user=config.MYSQL_USER,
    password=config.MYSQL_PASSWORD,
    database=config.MYSQL_DB_NAME
)
cursor = connection.cursor()

COLUMN_NAME = 'data'
TABLE_NAME = 'NewChatbot'

# Retrieve serialized JSON data from the MySQL table
cursor.execute(f"SELECT {COLUMN_NAME} FROM {TABLE_NAME}")
rows = cursor.fetchall()

# Iterate through each row
for row in rows:
    # Deserialize JSON data
    json_data = json.loads(row[0])

    # Create a new table
    create_table_query = "CREATE TABLE new_table (id INT AUTO_INCREMENT PRIMARY KEY"
    for key, value in json_data.items():
        # Check if the key is 'id', if so, escape it
        if key == 'id':
            key = '`data_id`'
        mysql_data_type = get_mysql_data_type(value)
        create_table_query += f", {key} {mysql_data_type}"
    create_table_query += ")"
    cursor.execute(create_table_query)

    # Insert rows into the new table
    insert_query = "INSERT INTO new_table ("
    insert_query += ", ".join(json_data.keys())
    insert_query += ") VALUES ("
    insert_query += ", ".join(["%s"] * len(json_data))
    insert_query += ")"
    cursor.execute(insert_query, tuple(json_data.values()))

# Commit changes and close connection
connection.commit()
connection.close()
