import datetime
import json
from json import JSONEncoder

import pandas as pd
import requests

from config import BASE_URL, SRC_CLIENT_DB_PATH, SRC_USER_DB_PATH
from service.database_service import (
    create_table_session,
    get_index_column_table_object,
    get_primary_key,
    get_sensitive_columns,
)
from service.sse_service import generate_hash_rows
from service.user_service import loginApi


class DateTimeEncoder(JSONEncoder):
    """
    This class encodes date time to json format.
    """

    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            print(obj)
            return obj.isoformat()
        elif isinstance(obj, (str)):
            print(obj)
            return obj.encode("utf-8")


def show_cloud_rows_hash(
    id_db: int, table_name: str, page: int, per_page: int, token: str
) -> list[dict]:
    """
    This function gets rows hashs of Cloud Database.

    Parameters
    ----------
    id_db : int
        Client Database ID.

    table_name : str
        Table name.

    page : int
        Page to paginate.

    per_page : int
        Rows number per query.

    token : str
        Acess token to acess API.

    Returns
    -------
    list[dict]
        Rows Hashs.
    """

    url = f"{BASE_URL}/showRowsHash"
    body = {"id_db": id_db, "table": table_name, "page": page, "per_page": per_page}

    header = {"Authorization": token}

    response = requests.post(url, json=body, headers=header)

    return response.json()


def insert_cloud_hash_rows(id_db: int, primary_key_list: list, table_name: str):
    # Get acess token
    token = loginApi()

    # Get sensitive columns names of Client Database
    sensitive_columns = get_sensitive_columns(id_db, table_name, token)

    # Add primary key in sensitive columns only to query
    sensitive_columns.append(get_primary_key(table_name=table_name))

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        SRC_CLIENT_DB_PATH, table_name, sensitive_columns
    )

    # Get index of primary key column to Client Database
    client_primary_key_index = get_index_column_table_object(
        table_client_db, get_primary_key(table_name=table_name)
    )

    # Get news rows to encrypted and send Cloud Database
    news_rows_client_db = []
    for primary_key in primary_key_list:
        result = (
            session_client_db.query(table_client_db)
            .filter(table_client_db.c[client_primary_key_index] == primary_key)
            .first()
        )

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
    url = f"{BASE_URL}/encryptDatabaseRows"
    body = {"id_db": id_db, "table": table_name, "rows_to_encrypt": news_rows_client_db}
    header = {"Authorization": token}
    response = requests.post(url, json=body, headers=header)

    if response.status_code != 200:
        return 400
    print("--- Encriptou as novas linhas ---")

    # Anonymization new row
    url = f"{BASE_URL}/anonymizationDatabaseRows"
    body = {
        "id_db": id_db,
        "table_name": table_name,
        "rows_to_anonymization": news_rows_client_db,
        "insert_database": 1,
    }
    header = {"Authorization": token}
    response = requests.post(url, json=body, headers=header)

    if response.status_code != 200:
        return 400
    session_client_db.commit()
    print("--- Anonimizou as novas linhas ---")

    # Get anonymized news rows to generate their hash
    anonymized_news_rows = []
    for primary_key in primary_key_list:
        result = (
            session_client_db.query(table_client_db)
            .filter(table_client_db.c[client_primary_key_index] == primary_key)
            .first()
        )

        anonymized_news_rows.append(list(result))

    # Generate hash of anonymized new rows
    generate_hash_rows(
        table_name=table_name,
        result_query=anonymized_news_rows,
    )
    print("--- Gerou hashs das novas linhas ---")

    # Create table object of User Database and
    # session of User Database to run sql operations
    table_user_db, session_user_db = create_table_session(
        SRC_USER_DB_PATH,
        table_name,
        [get_primary_key(table_name=table_name), "line_hash"],
    )
    session_user_db.commit()

    # Get index of primary key column to User Database
    user_primary_key_index = get_index_column_table_object(
        table_user_db, get_primary_key(table_name=table_name)
    )

    # Get hashs of anonymized news rows to insert Cloud Database
    user_rows_to_insert = []
    for primary_key in primary_key_list:
        result = (
            session_user_db.query(table_user_db)
            .filter(table_user_db.c[user_primary_key_index] == primary_key)
            .first()
        )

        user_rows_to_insert.append(result._asdict())

    # Include hash rows in Cloud Database
    url = f"{BASE_URL}/includeHashRows"
    body = {"id_db": id_db, "table": table_name, "hash_rows": user_rows_to_insert}
    header = {"Authorization": token}
    response = requests.post(url, json=body, headers=header)

    if response.status_code != 200:
        return 400
    print("--- Incluiu hashs das novas linhas ---")

    return 200


def process_updates(id_db, primary_key_list, table_name):
    url = f"{BASE_URL}/processUpdates"

    token = loginApi()

    if not token:
        return 400

    body = {
        "id_db": id_db,
        "table_name": table_name,
        "primary_key_list": primary_key_list,
    }

    header = {"Authorization": token}
    response = requests.post(url, json=body, headers=header)

    return

    # Get path of Client DataBase
    src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, HOST, PORT, NAME_DATABASE
    )

    # Get sensitive columns names of Client Database
    sensitive_columns = get_sensitive_columns(id_db, table_name, token)[
        "sensitive_columns"
    ]

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
        result = (
            session_client_db.query(table_client_db)
            .filter(table_client_db.c[client_primary_key_index] == primary_key)
            .first()
        )

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
    url = f"{BASE_URL}/encryptDatabaseRows"
    body = {
        "id_db": id_db,
        "table": table_name,
        "rows_to_encrypt": news_rows_client_db,
        "update_database": 1,
    }
    header = {"Authorization": token}
    response = requests.post(url, json=body, headers=header)

    if response.status_code != 200:
        return 400
    print("--- Encriptou as linhas atualizadas ---")

    # Creating connection with User Database
    src_user_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, HOST, PORT, "UserDB"
    )

    # Get updated rows to generate their hash
    updated_rows = []
    for primary_key in primary_key_list:
        result = (
            session_client_db.query(table_client_db)
            .filter(table_client_db.c[client_primary_key_index] == primary_key)
            .first()
        )

        updated_rows.append(list(result))
    print(updated_rows)

    # Generate hash of anonymized new rows
    generate_hash_rows(
        src_client_db_path=src_client_db_path,
        src_user_db_path=src_user_db_path,
        table_name=table_name,
        result_query=updated_rows,
    )
    print("--- Gerou hashs das linhas atualizadas ---")

    # Create table object of User Database and
    # session of User Database to run sql operations
    table_user_db, session_user_db = create_table_session(
        src_user_db_path, table_name, [PRIMARY_KEY, "line_hash"]
    )
    session_user_db.commit()

    # Get index of primary key column to User Database
    user_primary_key_index = get_index_column_table_object(table_user_db, PRIMARY_KEY)

    # Get hashs of anonymized news rows to insert Cloud Database
    user_rows_to_insert = []
    for primary_key in primary_key_list:
        result = (
            session_user_db.query(table_user_db)
            .filter(table_user_db.c[user_primary_key_index] == primary_key)
            .first()
        )

        user_rows_to_insert.append(result._asdict())

    # Include hash rows in Cloud Database
    url = f"{BASE_URL}/includeHashRows"
    body = {"id_db": id_db, "table": table_name, "hash_rows": user_rows_to_insert}
    header = {"Authorization": token}
    response = requests.post(url, json=body, headers=header)

    if response.status_code != 200:
        return 400
    print("--- Incluiu hashs das linhas atualizadas ---")

    return 200


def delete_cloud_hash_rows(id_db, primary_key_list, table_name):

    url = f"{BASE_URL}/processDeletions"

    token = loginApi()

    if not token:
        return 400

    body = {
        "id_db": id_db,
        "table_name": table_name,
        "primary_key_list": primary_key_list,
    }

    header = {"Authorization": token}

    response = requests.post(url, json=body, headers=header)

    return 200
