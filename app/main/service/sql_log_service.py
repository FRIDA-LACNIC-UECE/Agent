import datetime
import json
import time
from json import JSONEncoder

import pandas as pd
import requests

from app.main.config import app_config
from app.main.config_client import ConfigClient
from app.main.exceptions import ValidationException, DefaultException
from app.main.service.database_service import (
    get_tables_names,
)
from app.main.service.table_service import (
    create_table_connection,
    get_primary_key,
    get_sensitive_columns,
)
from app.main.service.sse_service import generate_hash_column, generate_hash_rows
from app.main.service.user_service import login_api


def inserts_log(database_id: int, table_name: str, primary_key_list: list) -> None:
    _, token = login_api()

    # Get primary key name
    primary_key_name = get_primary_key(table_name=table_name)

    # Create table object of Client Database
    client_table_connection = create_table_connection(
        database_url=ConfigClient.CLIENT_DATABASE_URL,
        table_name=table_name,
    )

    news_rows_client_database = []
    for primary_key in primary_key_list:
        result = (
            client_table_connection.session.query(client_table_connection.table)
            .filter(
                client_table_connection.get_column(column_name=primary_key_name)
                == primary_key
            )
            .first()
        )

        news_rows_client_database.append(list(result._asdict().values()))

    datetime_column_index = None
    for index, column in enumerate(news_rows_client_database[0]):
        if isinstance(column, datetime.date):
            datetime_column_index = index

    insert_log = f"INSERT INTO {table_name} "
    insert_log += "VALUES "
    for new_row in news_rows_client_database:
        if datetime_column_index is not None:
            new_row[datetime_column_index] = new_row[datetime_column_index].strftime(
                "%Y-%m-%d"
            )

        insert_log += f" {tuple(new_row)} "

    response = requests.post(
        url=f"{app_config.API_URL}/sql_log/database/{database_id}",
        json={"sql_command": insert_log},
        headers={"Authorization": token},
    )

    if response.status_code != 201:
        raise DefaultException("insert_log_not_saved", code=500)


def deletions_log(database_id: int, table_name: str, primary_key_list: list) -> None:
    _, token = login_api()

    primary_key_name = get_primary_key(table_name=table_name)

    primary_key_tuple = tuple(sorted(primary_key_list))

    deletion_log = (
        f"DELETE FROM {table_name} WHERE {primary_key_name} IN {primary_key_tuple}"
    )

    response = requests.post(
        url=f"{app_config.API_URL}/sql_log/database/{database_id}",
        json={"sql_command": deletion_log},
        headers={"Authorization": token},
    )

    if response.status_code != 201:
        raise DefaultException("deletion_log_not_saved", code=500)
