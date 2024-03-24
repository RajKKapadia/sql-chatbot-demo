import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_DB_NAME = os.getenv('MYSQL_DB_NAME')
MYSQL_TABLES = []
GPT_MODEL = os.getenv('GPT_MODEL')

cwd = os.getcwd()

DEFINITION_DIR = 'data_definition'
CSV_DIR = 'csv_data'

os.makedirs(
    os.path.join(
        cwd,
        DEFINITION_DIR
    ),
    exist_ok=True
)

for definition in os.listdir(CSV_DIR):
    file_data = definition.rsplit('.', 1)
    table_name = file_data[0]
    extention = file_data[1]
    if 'csv' in extention:
        MYSQL_TABLES.append(table_name)

ERROR_MESSAGE = 'We are facing an issue at this moment, please try after sometime.'
