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
from app.main.service.sse_service import generate_hash_column, generate_hash_rows
from app.main.service.user_service import login_api


def deletions_log(database_id: int, table_name: str, primary_key_list: list) -> None:

    primary_key_name = get_primary_key(table_name=table_name)

    primary_key_tuple = tuple(sorted(primary_key_list))

    deletion_log = (
        f"DELETE FROM {table_name} WHERE {primary_key_name} IN {primary_key_tuple}"
    )

    token = login_api()

    if not token:
        raise ValidationException(
            errors={"user": "user_invalid_data"},
            message="Input payload validation failed",
        )

    response = requests.post(
        url=f"{app_config.API_URL}/sql_log",
        json={"database_id": database_id, "sql_command": deletion_log},
        headers={"Authorization": token},
    )

    if response.status_code != 201:
        raise ValidationException(
            errors={"log": "log_invalid_data"},
            message="Input payload validation failed",
        )


def inserts_log(database_id: int, table_name: str, primary_key_list: list) -> None:

    table_client_database, session_client_database = create_table_session(
        database_url=ConfigClient.CLIENT_DATABASE_URL, table_name=table_name
    )

    primary_key_index = get_index_column_table_object(
        table_object=table_client_database,
        column_name=get_primary_key(table_name=table_name),
    )

    news_rows_client_database = []
    for primary_key in primary_key_list:
        result = (
            session_client_database.query(table_client_database)
            .filter(table_client_database.c[primary_key_index] == primary_key)
            .first()
        )

        news_rows_client_database.append(list(result._asdict().values()))

    datetime_column_index = None
    for index, column in enumerate(news_rows_client_database[0]):
        if isinstance(column, datetime.date):
            datetime_column_index = index

    insert_log = f"INSERT INTO {table_name} \n"
    insert_log += "VALUES "
    for new_row in news_rows_client_database:
        if datetime_column_index is not None:
            new_row[datetime_column_index] = new_row[datetime_column_index].strftime(
                "%Y-%m-%d"
            )

        insert_log += f"\t{tuple(new_row)} "

    token = login_api()

    if not token:
        raise ValidationException(
            errors={"user": "user_invalid_data"},
            message="Input payload validation failed",
        )

    response = requests.post(
        url=f"{app_config.API_URL}/sql_log",
        json={"database_id": database_id, "sql_command": insert_log},
        headers={"Authorization": token},
    )

    if response.status_code != 201:
        raise ValidationException(
            errors={"log": "log_invalid_data"},
            message="Input payload validation failed",
        )
