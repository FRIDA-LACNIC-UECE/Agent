import datetime
import json
import time
from json import JSONEncoder

import pandas as pd
import requests

from app.main.config import app_config
from app.main.config_client import ConfigClient
from app.main.exceptions import ValidationException
from app.main.service.database_service import (
    create_table_session,
    get_index_column_table_object,
    get_primary_key,
    get_sensitive_columns,
    get_tables_names,
)
from app.main.service.sql_log_service import deletions_log, inserts_log
from app.main.service.sse_service import generate_hash_column, generate_hash_rows
from app.main.service.user_service import login_api


class DateTimeEncoder(JSONEncoder):
    """
    This class encodes date time to json format.
    """

    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, (str)):
            return obj.encode("utf-8")


def paginate_user_database(database_session, table_object, page, per_page):

    # Get data in User Database
    query = database_session.query(table_object).filter(
        table_object.c[0] >= (page * per_page),
        table_object.c[0] <= ((page + 1) * per_page),
    )

    results_user_data = {}
    results_user_data["primary_key"] = []
    results_user_data["row_hash"] = []

    for row in query:
        results_user_data["primary_key"].append(row[0])
        results_user_data["row_hash"].append(row[1])

    return results_user_data


def show_cloud_rows_hash(
    database_id: int, table_name: str, page: int, per_page: int, token: str
) -> list[dict]:

    response = requests.get(
        url=f"{app_config.API_URL}/agent/show_row_hash/{database_id}?table_name={table_name}&page={page}&per_page={per_page}",
        headers={"Authorization": token},
    )

    return response.json()


def insert_cloud_hash_rows(database_id: int, primary_key_list: list, table_name: str):

    # Get acess token
    token = login_api()

    # Get sensitive columns names of Client Database
    sensitive_columns = get_sensitive_columns(
        database_id=database_id, table_name=table_name, token=token
    )

    # Add primary key in sensitive columns only to query
    sensitive_columns.append(get_primary_key(table_name=table_name))

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_database, session_client_database = create_table_session(
        ConfigClient.CLIENT_DATABASE_URL, table_name, sensitive_columns
    )

    # Get index of primary key column to Client Database
    client_primary_key_index = get_index_column_table_object(
        table_object=table_client_database,
        column_name=get_primary_key(table_name=table_name),
    )

    # Get news rows to encrypted and send Cloud Database
    news_rows_client_db = []
    for primary_key in primary_key_list:
        result = (
            session_client_database.query(table_client_database)
            .filter(table_client_database.c[client_primary_key_index] == primary_key)
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
    response = requests.post(
        url=f"{app_config.API_URL}/encryption/database_rows/{database_id}",
        json={
            "table_name": table_name,
            "rows_to_encrypt": news_rows_client_db,
            "update_database": False,
        },
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise ValidationException(
            errors={"database": "database_invalid_data"},
            message="Input payload validation failed",
        )

    print("--- Encriptou as novas linhas ---")

    # Anonymization new row
    response = requests.post(
        url=f"{app_config.API_URL}/anonymization/database_rows/{database_id}",
        json={
            "table_name": table_name,
            "rows_to_anonymization": news_rows_client_db,
            "insert_database": True,
        },
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise ValidationException(
            errors={"database": "database_invalid_data"},
            message="Input payload validation failed",
        )

    session_client_database.commit()

    print("--- Anonimizou as novas linhas ---")

    # Get anonymized news rows to generate their hash
    anonymized_news_rows = []
    for primary_key in primary_key_list:
        result = (
            session_client_database.query(table_client_database)
            .filter(table_client_database.c[client_primary_key_index] == primary_key)
            .first()
        )

        anonymized_news_rows.append(list(result))

    print(f"anonymized_news_rows = {anonymized_news_rows}")

    # Generate hash of anonymized new rows
    generate_hash_rows(
        table_name=table_name,
        result_query=anonymized_news_rows,
    )

    print("--- Gerou hashs das novas linhas ---")

    # Create table object of User Database and
    # session of User Database to run sql operations
    table_agent_database, session_agent_database = create_table_session(
        database_url=ConfigClient.AGENT_DATABASE_URL,
        table_name=table_name,
        columns_list=["primary_key", "line_hash"],
    )
    session_agent_database.commit()

    # Get hashs of anonymized news rows to insert Cloud Database
    agent_rows_to_insert = []
    for primary_key in primary_key_list:
        result = (
            session_agent_database.query(table_agent_database)
            .where(table_agent_database.c[0] == primary_key)
            .first()
        )

        agent_rows_to_insert.append(result._asdict())

    print(f"agent_rows_to_insert = {agent_rows_to_insert}")

    # Include hash rows in Cloud Database
    response = requests.post(
        url=f"{app_config.API_URL}/agent/include_hash_rows/{database_id}",
        json={"table_name": table_name, "hash_rows": agent_rows_to_insert},
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise ValidationException(
            errors={"database": "database_invalid_data"},
            message="Input payload validation failed",
        )

    print("--- Incluiu hashs das novas linhas ---")


def process_updates(database_id, primary_key_list, table_name):
    token = login_api()

    if not token:
        raise ValidationException(
            errors={"user": "user_invalid_data"},
            message="Input payload validation failed",
        )

    response = requests.post(
        url=f"{app_config.API_URL}/agent/process_updates/{database_id}",
        json={"table_name": table_name, "primary_key_list": primary_key_list},
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise ValidationException(
            errors={"database": "database_invalid_data"},
            message="Input payload validation failed",
        )


def process_deletions(database_id, table_name, primary_key_list):

    token = login_api()

    if not token:
        raise ValidationException(
            errors={"user": "user_invalid_data"},
            message="Input payload validation failed",
        )

    response = requests.post(
        url=f"{app_config.API_URL}/agent/process_deletions/{database_id}",
        json={"table_name": table_name, "primary_key_list": primary_key_list},
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise ValidationException(
            errors={"database": "database_invalid_data"},
            message="Input payload validation failed",
        )


def checking_changes() -> int:
    """
    This function checks client database changes.

    Parameters
    ----------
        No parameters
    Returns
    -------
    int
        status code.
    """

    print("\n===== Initializing verification:")

    # Generate rows hash each table
    for table_name in get_tables_names(database_url=ConfigClient.CLIENT_DATABASE_URL):

        print(f"\n===== {table_name} =====")

        try:
            generate_hash_column(table_name=table_name)
        except:
            print("table_not_ready_verification")
            print("\nFinalizing Verification =====\n")
            return

        # Get acess token
        token = login_api()

        # Start number page
        page = 0

        # Start size page
        per_page = 100

        # Create table object of User Database and
        # session of User Database to run sql operations
        table_user_db, session_user_db = create_table_session(
            database_url=ConfigClient.AGENT_DATABASE_URL, table_name=table_name
        )

        # Get data in Cloud Database
        response_show_cloud_hash_rows = show_cloud_rows_hash(
            database_id=ConfigClient.CLIENT_DATABASE_ID,
            table_name=table_name,
            page=page,
            per_page=per_page,
            token=token,
        )
        results_cloud_data = response_show_cloud_hash_rows["row_hash_list"][0]
        primary_key_value_min_limit = response_show_cloud_hash_rows[
            "primary_key_value_min_limit"
        ]
        primary_key_value_max_limit = response_show_cloud_hash_rows[
            "primary_key_value_max_limit"
        ]

        # Get data in User Database
        results_user_data = paginate_user_database(
            session_user_db, table_user_db, page, per_page
        )

        # Transforme to set
        set_user_hash = set(results_user_data["row_hash"])
        set_cloud_hash = set(results_cloud_data["row_hash"])

        diff_ids_user = []
        diff_ids_cloud = []

        # Get data in User Database and Cloud Database
        while (page * per_page) <= (primary_key_value_max_limit):

            # Get differences between User Database and Cloud Database
            diff_hashs_user = list(set_user_hash.difference(set_cloud_hash))

            for diff_hash in diff_hashs_user:
                diff_index = results_user_data["row_hash"].index(diff_hash)
                diff_ids_user.append(results_user_data["primary_key"][diff_index])

            diff_hashs_cloud = list(set_cloud_hash.difference(set_user_hash))

            for diff_hash in diff_hashs_cloud:
                diff_index = results_cloud_data["row_hash"].index(diff_hash)
                diff_ids_cloud.append(results_cloud_data["primary_key"][diff_index])

            page += 1

            # Get data in Cloud Database
            results_cloud_data = show_cloud_rows_hash(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                page=page,
                per_page=per_page,
                token=token,
            )["row_hash_list"][0]

            # Get data in User Database
            results_user_data = paginate_user_database(
                session_user_db, table_user_db, page, per_page
            )

            # Transforme to set
            set_user_hash = set(results_user_data["row_hash"])
            set_cloud_hash = set(results_cloud_data["row_hash"])

        # Get differences between user database and cloud database
        diff_ids_user = set(diff_ids_user)
        diff_ids_cloud = set(diff_ids_cloud)

        # Get differences (add, update, remove)
        insert_ids = list(diff_ids_user.difference(diff_ids_cloud))
        update_ids = list(diff_ids_user.intersection(diff_ids_cloud))
        delete_ids = list(diff_ids_cloud.difference(diff_ids_user))

        print(f"Insert IDs -> {insert_ids}")
        print(f"Update IDs -> {update_ids}")
        print(f"Delete IDs -> {delete_ids}")

        # Insert news rows on cloud database
        if len(insert_ids) != 0:
            inserts_log(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=insert_ids,
            )
            insert_cloud_hash_rows(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=insert_ids,
            )

        # Process updates
        if len(update_ids) != 0:
            process_updates(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=update_ids,
            )

        # Delete rows on cloud database
        if len(delete_ids) != 0:
            deletions_log(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=delete_ids,
            )
            process_deletions(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=delete_ids,
            )

    session_user_db.commit()
    session_user_db.close()

    return 200


def create_verification_thread():
    """
    This function create thread to check client database changes.

    Parameters
    ----------
        No parameters
    Returns
    -------
    int
        status code.
    """

    # Set time period of task
    seconds_task = 30

    # Running Task
    while True:
        # Checking runtime
        checking_changes()
        time.sleep(seconds_task)
