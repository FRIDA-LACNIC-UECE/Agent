import hashlib
import pandas as pd
import requests
from sqlalchemy import insert, select, update
from app.main.config import app_config
from app.main.config_client import ConfigClient
from app.main.exceptions import DefaultException
from app.main.service.table_service import TableConnection


def show_cloud_rows_hash(
    database_id: int, table_name: str, page: int, per_page: int, token: str
) -> dict[str, any]:
    table = get_table(database_id=database_id, table_name=table_name, token=token)

    response = requests.get(
        url=f"{app_config.API_URL}/agent/show_row_hash/database/{database_id}/table/{table['id']}?page={page}&per_page={per_page}",
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise DefaultException("internal_error_getting_cloud_rows_hashs", code=500)

    return response.json()


def include_hash_column(
    table_connection: TableConnection,
    primary_key_data: list,
    raw_data: list,
) -> None:
    for primary_key, row in zip(primary_key_data, range(raw_data.shape[0])):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        statement = insert(table_connection.table).values(
            primary_key=primary_key, line_hash=hashed_line
        )

        table_connection.session.execute(statement)

    table_connection.session.commit()


def update_hash_column(
    table_connection: TableConnection,
    primary_key_data: list,
    raw_data: list,
) -> None:
    for primary_key, row in zip(primary_key_data, range(raw_data.shape[0])):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        statement = (
            update(table_connection.table)
            .where(
                table_connection.get_column(column_name="primary_key") == primary_key
            )
            .values(line_hash=hashed_line)
        )

        table_connection.session.execute(statement)

    table_connection.session.commit()


def generate_hash_rows(table_name, result_query):
    # Get acess token
    _, token = login_api()

    # Get primary key name
    primary_key_name = get_primary_key(table_name=table_name)

    # Get sensitive columns of table
    sensitive_columns = [primary_key_name] + get_sensitive_columns(
        ConfigClient.CLIENT_DATABASE_ID, table_name, token
    )

    # Create table object of Agent Database
    agent_table_connection = create_table_connection(
        database_url=ConfigClient.AGENT_DATABASE_URL,
        table_name=table_name,
        columns_list=["table_connection", "line_hash"],
    )

    # Update hash column
    raw_data = pd.DataFrame(data=result_query, columns=sensitive_columns)
    primary_key_data = raw_data[primary_key_name]
    raw_data.pop(primary_key_name)

    update_hash_column(
        table_connection=agent_table_connection,
        primary_key_data=primary_key_data,
        raw_data=raw_data,
    )

    agent_table_connection.session.commit()
    agent_table_connection.close()


def generate_hash_column(table_name):
    # Get acess token
    _, token = login_api()

    # Get primary key name
    primary_key_name = get_primary_key(table_name=table_name)

    # Get sensiyibe column names along with primary key name
    client_columns_list = [primary_key_name] + get_sensitive_columns(
        ConfigClient.CLIENT_DATABASE_ID, table_name, token
    )

    # Create table connection of Client Database
    client_table_connection = create_table_connection(
        database_url=ConfigClient.CLIENT_DATABASE_URL,
        table_name=table_name,
        columns_list=client_columns_list,
    )

    # Create table connection of Agent Database
    agent_table_connection = create_table_connection(
        database_url=ConfigClient.AGENT_DATABASE_URL,
        table_name=table_name,
    )

    # Delete all rows of agent database table
    agent_table_connection.session.query(agent_table_connection.table).delete()
    agent_table_connection.session.commit()

    # Generate hashs
    size = 100
    statement = select(client_table_connection.table)
    results_proxy = client_table_connection.session.execute(
        statement
    )  # Proxy to get data on batch
    results = results_proxy.fetchmany(size)  # Getting data

    while results:
        from_db = []

        for result in results:
            from_db.append(list(result))

        raw_data = pd.DataFrame(from_db, columns=client_columns_list)
        primary_key_data = raw_data[primary_key_name]
        raw_data.pop(primary_key_name)

        # Getting data
        results = results_proxy.fetchmany(size)

        include_hash_column(
            table_connection=agent_table_connection,
            primary_key_data=primary_key_data,
            raw_data=raw_data,
        )

    client_table_connection.session.commit()
    agent_table_connection.session.commit()

    client_table_connection.close()
    agent_table_connection.close()


from app.main.service.user_service import login_api
from app.main.service.table_service import (
    get_table,
    create_table_connection,
    get_primary_key,
    get_sensitive_columns,
)
