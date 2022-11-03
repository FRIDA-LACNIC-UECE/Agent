import requests

import pandas as pd

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE, BASE_URL, PRIMARY_KEY
)

from service.user_service import loginApi
from service.database_service import (
    get_index_column_table_object, create_table_session,
    get_sensitive_columns
)


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


def insert_cloud_hash_rows(id_db, primary_key_list, table):
    # Get path of Client DataBase
    src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, NAME_DATABASE
    )

    # Get acess token
    token = loginApi()
    
    # Get sensitive columns names of Client Database
    sensitive_columns = get_sensitive_columns(id_db, token)['sensitive_columns']
    print(f"sensitive_columns = {sensitive_columns}")

    # Add primary key in sensitive columns only to query
    sensitive_columns.append(PRIMARY_KEY)
    print(sensitive_columns)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_client_db_path, table, sensitive_columns
    )
    
    # Get index of primary key column to Client Database
    client_primary_key_index = get_index_column_table_object(
        table_client_db, PRIMARY_KEY
    )
    
    # Get database rows to insert on Client Database
    client_rows_to_insert = []
    for primary_key in primary_key_list:
        result = session_client_db.query(table_client_db).filter(
            table_client_db.c[client_primary_key_index] == primary_key
        ).first()
        
        client_rows_to_insert.append(result._asdict())
    print(client_rows_to_insert)

    # Encrypt new rows and send Cloud Database
    url = f'{BASE_URL}/encryptDatabaseRows'
    body = {
        "id_db": id_db,
        "table": table,
        "rows_to_encrypt": client_rows_to_insert
    }

    header = {"Authorization": token}

    response = requests.post(url, json=body, headers=header)

    if response.status_code != 200:
        print("Deu ruim 1")
        return 400

    print(response.json())

    # Creating connection with User Database
    src_user_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, "UserDB"
    )

    # Create table object of User Database and 
    # session of User Database to run sql operations
    table_user_db, session_user_db = create_table_session(
        src_user_db_path, table, [PRIMARY_KEY, "line_hash"]
    )

    # Get index of primary key column to User Database
    user_primary_key_index = get_index_column_table_object(
        table_user_db, PRIMARY_KEY
    )
    
    # Get database rows to insert on User Database
    user_rows_to_insert = []
    for primary_key in primary_key_list:
        result = session_user_db.query(table_user_db).filter(
            table_user_db.c[user_primary_key_index] == primary_key
        ).first()
        
        user_rows_to_insert.append(result._asdict())
    print(user_rows_to_insert)

    # Include hash rows in Cloud Database
    url = f'{BASE_URL}/includeHashRows'
    body = {
        "id_db": id_db,
        "table": table,
        "hash_rows": user_rows_to_insert
    }

    header = {"Authorization": token}

    response = requests.post(url, json=body, headers=header)

    if response.status_code != 200:
        print("Deu ruim 2")
        return 400

    print(pd.DataFrame(data=client_rows_to_insert))

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