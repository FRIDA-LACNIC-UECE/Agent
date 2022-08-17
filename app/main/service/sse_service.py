import hashlib

import pandas as pd
import requests
from sqlalchemy import insert, select, update

from app.main.config import app_config
from app.main.config_client import ConfigClient
from app.main.service.database_service import (
    create_table_session,
    get_index_column_table_object,
    get_primary_key,
    get_sensitive_columns,
)
from app.main.service.user_service import login_api


def include_hash_column(
    session,
    table_object,
    primary_key_data,
    raw_data,
):

    for (primary_key, row) in zip(primary_key_data, range(raw_data.shape[0])):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        stmt = insert(table_object).values(
            primary_key=primary_key, line_hash=hashed_line
        )

        session.execute(stmt)

    session.commit()
    session.close()


def update_hash_column(
    session_agent_database,
    table_object_agent_database,
    primary_key_data,
    raw_data,
):

    # Update hash column
    for (primary_key, row) in zip(primary_key_data, range(raw_data.shape[0])):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        stmt = (
            update(table_object_agent_database)
            .where(table_object_agent_database.c[0] == primary_key)
            .values(line_hash=hashed_line)
        )

        session_agent_database.execute(stmt)

    session_agent_database.commit()
    session_agent_database.close()


def generate_hash_rows(table_name, result_query):

    # Get acess token
    token = login_api()

    # Get primary key name
    primary_key_name = get_primary_key(table_name=table_name)

    # Get sensitive columns of table
    sensitive_columns = [primary_key_name] + get_sensitive_columns(
        ConfigClient.CLIENT_DATABASE_ID, table_name, token
    )

    # Create table object of Agent Database and
    # session of Agent Database to run sql operations
    table_object_agent_database, session_agent_database = create_table_session(
        database_url=ConfigClient.AGENT_DATABASE_URL,
        table_name=table_name,
        columns_list=["primary_key", "line_hash"],
    )

    raw_data = pd.DataFrame(data=result_query, columns=sensitive_columns)
    primary_key_data = raw_data[primary_key_name]
    raw_data.pop(primary_key_name)

    update_hash_column(
        session_agent_database=session_agent_database,
        table_object_agent_database=table_object_agent_database,
        primary_key_data=primary_key_data,
        raw_data=raw_data,
    )


def generate_hash_column(table_name):

    # Get acess token
    token = login_api()

    # Get primary key name
    primary_key_name = get_primary_key(table_name=table_name)

    # Get sensitive columns of table
    sensitive_columns = get_sensitive_columns(
        ConfigClient.CLIENT_DATABASE_ID, table_name, token
    )

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    client_table_object, client_session = create_table_session(
        database_url=ConfigClient.CLIENT_DATABASE_URL,
        table_name=table_name,
        columns_list=[primary_key_name] + sensitive_columns,
    )

    # Create table object of Agent Database and
    # session of Agent Database to run sql operations
    agent_table_object, agent_session = create_table_session(
        database_url=ConfigClient.AGENT_DATABASE_URL,
        table_name=table_name,
    )

    # Delete all rows of agent database table
    agent_session.query(agent_table_object).delete()
    agent_session.commit()
    agent_session.close()

    # Generate hashs
    size = 1000
    statement = select(client_table_object)
    results_proxy = client_session.execute(statement)  # Proxy to get data on batch
    results = results_proxy.fetchmany(size)  # Getting data

    while results:
        from_db = []

        for result in results:
            from_db.append(list(result))

        client_session.close()

        raw_data = pd.DataFrame(from_db, columns=[primary_key_name] + sensitive_columns)
        primary_key_data = raw_data[primary_key_name]
        raw_data.pop(primary_key_name)

        results = results_proxy.fetchmany(size)  # Getting data

        include_hash_column(
            session=agent_session,
            table_object=agent_table_object,
            primary_key_data=primary_key_data,
            raw_data=raw_data,
        )
