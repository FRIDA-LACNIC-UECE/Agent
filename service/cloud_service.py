import requests

import pandas as pd

import json
import datetime
from json import JSONEncoder

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE, BASE_URL, PRIMARY_KEY
)

from service.user_service import loginApi
from service.database_service import (
    get_index_column_table_object, create_table_session,
    get_sensitive_columns
)
from service.sse_service import generate_hash_rows


# subclass JSONEncoder
class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            print(obj)
            return obj.isoformat()
        elif isinstance(obj, (str)):
            print(obj)
            return obj.encode("utf-8")


def get_id_cloud_database():

    url = f'{BASE_URL}/getDatabases'

    token = loginApi()

    if not token:
        return None, 400

    header = {"Authorization": token}
    databases_response = requests.get(url, headers=header)

    id_db = None
    for database in databases_response.json():
        if (
            database["name_db_type"] == TYPE_DATABASE and 
            database["user"] == USER_DATABASE and
            database["password"] == PASSWORD_DATABASE and
            database["host"] == HOST and
            database["port"] == int(PORT) and
            database["name"] == NAME_DATABASE
        ):
            id_db = database["id"]
            
            return id_db, 200

    if id_db == None:
        return None, 404


def show_cloud_hash_rows(id_db, table, page, per_page, token):

    url = f'{BASE_URL}/showHashRows'
    body = {
        "id_db": id_db,
        "table": table,
        "page": page,
        "per_page": per_page
    }

    header = {"Authorization": token}

    response = requests.post(url, json=body, headers=header)

    return response.json()


def insert_cloud_hash_rows(id_db, primary_key_list, table_name):
    # Get path of Client DataBase
    src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, NAME_DATABASE
    )

    # Get acess token
    token = loginApi()
    
    # Get sensitive columns names of Client Database
    sensitive_columns = get_sensitive_columns(id_db, token)['sensitive_columns']

    # Add primary key in sensitive columns only to query
    sensitive_columns.append(PRIMARY_KEY)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_client_db_path, table_name, sensitive_columns
    )
    
    # Get index of primary key column to Client Database
    client_primary_key_index = get_index_column_table_object(
        table_client_db, PRIMARY_KEY
    )
    
    # Get news rows to encrypted and send Cloud Database
    news_rows_client_db = []
    for primary_key in primary_key_list:
        result = session_client_db.query(table_client_db).filter(
            table_client_db.c[client_primary_key_index] == primary_key
        ).first()
        
        news_rows_client_db.append(result._asdict())

    # Get date type columns on news rows of Client Database
    first_row = news_rows_client_db[0]

    data_type_keys = []
    for key in first_row.keys():
        type_data = str(type(first_row[key]).__name__)
        if type_data == "date":
            data_type_keys.append(key)
    
    # Convert from date type to string
    for row in news_rows_client_db:
        for key in data_type_keys:
            row[key] = row[key].strftime("%Y-%m-%d")

    # Encrypt new rows and send Cloud Database
    url = f'{BASE_URL}/encryptDatabaseRows'
    body = {
        "id_db": id_db,
        "table": table_name,
        "rows_to_encrypt": news_rows_client_db
    }
    header = {"Authorization": token}
    response = requests.post(url, json=body, headers=header)
    
    if response.status_code != 200:
        return 400
    print("--- Encriptou as novas linhas ---")

    # Creating connection with User Database
    src_user_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, "UserDB"
    )

    # Anonymization new row
    url = f'{BASE_URL}/anonymizationDatabaseRows'
    body = {
        "id_db": id_db,
        "table_name": table_name,
        "rows_to_anonymization": news_rows_client_db
    }
    header = {"Authorization": token}
    response = requests.post(url, json=body, headers=header)

    if response.status_code != 200:
        return 400
    session_client_db.commit()
    print("--- Anonimizou as novas linhas ---")

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_client_db_path, table_name
    )

    # Get anonymized news rows to generate their hash
    anonymized_news_rows = []
    for primary_key in primary_key_list:
        result = session_client_db.query(table_client_db).filter(
            table_client_db.c[client_primary_key_index] == primary_key
        ).first()
        
        anonymized_news_rows.append(list(result))
    print(anonymized_news_rows)

    # Generate hash of anonymized new rows
    generate_hash_rows(
        src_client_db_path=src_client_db_path, 
        src_user_db_path=src_user_db_path,
        table_name=table_name,
        result_query=anonymized_news_rows
    )
    print("--- Gerou hashs das novas linhas ---")
    

    # Create table object of User Database and 
    # session of User Database to run sql operations
    table_user_db, session_user_db = create_table_session(
        src_user_db_path, table_name, [PRIMARY_KEY, "line_hash"]
    )
    session_user_db.commit()

    # Get index of primary key column to User Database
    user_primary_key_index = get_index_column_table_object(
        table_user_db, PRIMARY_KEY
    )
    
    # Get hashs of anonymized news rows to insert Cloud Database
    user_rows_to_insert = []
    for primary_key in primary_key_list:
        result = session_user_db.query(table_user_db).filter(
            table_user_db.c[user_primary_key_index] == primary_key
        ).first()
        
        user_rows_to_insert.append(result._asdict())
    
    # Include hash rows in Cloud Database
    url = f'{BASE_URL}/includeHashRows'
    body = {
        "id_db": id_db,
        "table": table_name,
        "hash_rows": user_rows_to_insert
    }
    header = {"Authorization": token}
    response = requests.post(url, json=body, headers=header)

    if response.status_code != 200:
        return 400
    print("--- Incluiu hashs das novas linhas ---")

    print(pd.DataFrame(data=news_rows_client_db))

    print("\n+++\n")

    return 200


def delete_cloud_hash_rows(id_db, primary_key_list, table):

    url = f'{BASE_URL}/deleteRowsHash'

    token = loginApi()

    if not token:
        return 400

    body = {
        "id_db": id_db, 
        "primary_key_list": primary_key_list,
        "table": table
    }

    header = {"Authorization": token}

    response = requests.post(url, json=body, headers=header)
    
    return 200