import os
import re
from typing import Any
import json

import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from sqlalchemy_utils import database_exists, create_database

import config

engine = create_engine(
    f'mysql+mysqlconnector://{config.MYSQL_USER}:{config.MYSQL_PASSWORD}@{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_DB_NAME}')


def remove_special_characters(text):
    pattern = r'[^\w\s]'
    return re.sub(pattern, '', text)


def clean_column_names(column_name: str) -> str:
    cleaned_name = column_name.replace(' ', '_')
    cleaned_name = re.sub(r'[^\w_]', '', cleaned_name)
    return cleaned_name


def clean_price_column(value: str) -> str:
    pattern = r"\b(\d+)\.00\b"
    match = re.search(pattern, value)
    if match:
        digits_before_decimal = match.group(1)
        return digits_before_decimal
    return value


def is_valid_date(col: list) -> bool:
    date_string = str(col[0])
    patterns = [
        r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$',  # YYYY-MM-DD
        # DD-MMM-YYYY
        r'^(0[1-9]|[12][0-9]|3[01])-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4}$',
        # MMM DD YYYY
        r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (0[1-9]|[12][0-9]|3[01]) \d{4}$',
        # YYYY-MMM-DD
        r'^\d{4}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(0[1-9]|[12][0-9]|3[01])$',
        # YYYY-MM-DD HH:MM:SS
        r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) (\d{2}):(\d{2}):(\d{2})$',
        # DD-MMM-YYYY HH:MM:SS
        r'^(0[1-9]|[12][0-9]|3[01])-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4} (\d{2}):(\d{2}):(\d{2})$',
        # MMM DD YYYY HH:MM:SS
        r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (0[1-9]|[12][0-9]|3[01]) \d{4} (\d{2}):(\d{2}):(\d{2})$',
        # YYYY-MMM-DD HH:MM:SS
        r'^\d{4}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(0[1-9]|[12][0-9]|3[01]) (\d{2}):(\d{2}):(\d{2})$',
        # DD-MM-YYYY HH:MM:SS
        r'^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-\d{4} (\d{2}):(\d{2}):(\d{2})$',
    ]
    for pattern in patterns:
        if re.match(pattern, date_string):
            return True
    return False


def convert_to_datetime(col: str) -> Any:
    try:
        if is_valid_date(col):
            return pd.to_datetime(col)
        return col
    except ValueError:
        return col


def get_column_names(cnx: mysql.connector.MySQLConnection, table_name: str) -> list[str]:
    cursor = cnx.cursor()
    column_names = []
    cursor.execute(
        f"SELECT * FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='{config.MYSQL_DB_NAME}' AND `TABLE_NAME`='{table_name}';")
    for col in cursor:
        column_names.append([col[3], col[7]])
    cursor.close()
    return column_names


print('Checking DB.')

if not database_exists(engine.url):
    create_database(engine.url)

print(f'DB exists - {database_exists(engine.url)}.')

cnx = mysql.connector.connect(user=config.MYSQL_USER, password=config.MYSQL_PASSWORD,
                              host=config.MYSQL_HOST, port=config.MYSQL_PORT, database=config.MYSQL_DB_NAME)

print('Reading the CSV files.')

CSV_DIR = 'csv_data'
DEFINITION_DIR = 'data_definition'

csv_files = os.listdir(CSV_DIR)

for csv_file in csv_files:
    if os.path.isfile(os.path.join(CSV_DIR, csv_file)):
        file_name = csv_file.rsplit('.', 1)[0]
        file_extention = csv_file.rsplit('.', 1)[1]
        if 'csv' in file_extention:
            new_file_name = remove_special_characters(file_name)
            new_file_name = ''.join(char.lower()
                                    for char in new_file_name if not char.isdigit())
            new_file_name = new_file_name.replace(' ', '_')
            new_file_name += f'.{file_extention}'
            new_file_path = os.path.join(CSV_DIR, new_file_name)
            os.rename(os.path.join(CSV_DIR, csv_file), new_file_path)
            print(f"Renamed '{csv_file}' to '{new_file_name}'.")

for csv_file in csv_files:
    if os.path.isfile(os.path.join(CSV_DIR, csv_file)):
        file_name = csv_file.rsplit('.', 1)[0]
        file_extention = csv_file.rsplit('.', 1)[1]
        if 'csv' in file_extention:
            csv_file_path = os.path.join(CSV_DIR, csv_file)
            print(f'Working on the {csv_file_path}.')
            data = pd.read_csv(csv_file_path)
            print('Cleaning CSV data.')
            data.columns = [clean_column_names(col) for col in data.columns]
            for col in data.columns:
                data[col] = convert_to_datetime(data[col])
            print('Creating table and pushing the data.')
            table_name = csv_file.rsplit('.', 1)[0]
            data.to_sql(table_name, engine, if_exists='replace',
                        index=False, method='multi', chunksize=100)
            print('Creating empty data definitions.')
            data_definitions = {
                "tables": [
                    {
                        "name": table_name,
                        "description": "",
                        "columns": []
                    }
                ]
            }
            columns = get_column_names(cnx, table_name)
            for column in columns:
                data_definitions['tables'][0]['columns'].append(
                    {
                        "name": str(column[0]),
                        "type": str(column[1]),
                        "description": ""
                    }
                )
            print('Writing data definitions.')
            with open(os.path.join(DEFINITION_DIR, f'{table_name}.json'), 'w') as file:
                file.write(json.dumps(data_definitions, indent=2))
            print(f'Finished working on the {csv_file_path}.')
