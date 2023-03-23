import hashlib

import pandas as pd
from sqlalchemy import insert, select, update

from config import ID_DB, SRC_CLIENT_DB_PATH, SRC_USER_DB_PATH
from service.database_service import (
    create_table_session,
    get_primary_key,
    get_sensitive_columns,
)
from service.user_service import loginApi


def include_hash_column(
    session_user_db,
    table_user_db,
    primary_key_data,
    raw_data,
):

    for (primary_key, row) in zip(primary_key_data, range(raw_data.shape[0])):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        stmt = insert(table_user_db).values(id=primary_key, line_hash=hashed_line)

        session_user_db.execute(stmt)

    session_user_db.commit()
    session_user_db.close()


def update_hash_column(
    session_user_db,
    table_user_db,
    primary_key_data,
    raw_data,
):
    for (primary_key, row) in zip(primary_key_data, range(raw_data.shape[0])):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        stmt = (
            update(table_user_db)
            .where(table_user_db.c[0] == primary_key)
            .values(line_hash=hashed_line)
        )

        session_user_db.execute(stmt)

    session_user_db.commit()
    session_user_db.close()


def generate_hash_rows(table_name, result_query):

    # Get acess token
    token = loginApi()

    # Get primary key name
    primary_key_name = get_primary_key(table_name=table_name)

    # Get sensitive columns of table
    sensitive_columns = get_sensitive_columns(ID_DB, table_name, token)
    client_columns_list = [primary_key_name] + sensitive_columns
    print(client_columns_list)

    # Create table object of User Database and
    # session of User Database to run sql operations
    table_user_db, session_user_db = create_table_session(
        src_db_path=SRC_USER_DB_PATH,
        table=table_name,
        columns_list=[primary_key_name, "line_hash"],
    )

    raw_data = pd.DataFrame(data=result_query, columns=client_columns_list)
    primary_key_data = raw_data[primary_key_name]
    raw_data.pop(primary_key_name)

    update_hash_column(
        session_user_db=session_user_db,
        table_user_db=table_user_db,
        primary_key_data=primary_key_data,
        raw_data=raw_data,
    )


def generate_hash_column(table_name):

    # Get acess token
    token = loginApi()

    # Get primary key name
    primary_key_name = get_primary_key(table_name=table_name)

    # Get sensitive columns of table
    sensitive_columns = get_sensitive_columns(ID_DB, table_name, token)
    client_columns_list = [primary_key_name] + list(sensitive_columns)

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=SRC_CLIENT_DB_PATH,
        table=table_name,
        columns_list=client_columns_list,
    )

    # Create table object of User Database and
    # session of User Database to run sql operations
    table_user_db, session_user_db = create_table_session(
        src_db_path=SRC_USER_DB_PATH,
        table=table_name,
        columns_list=[primary_key_name, "line_hash"],
    )

    # Delete all rows of Cloud Database Table
    session_user_db.query(table_user_db).delete()
    session_user_db.commit()
    session_user_db.close()

    # Generate hashs
    size = 1000
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(statement)  # Proxy to get data on batch
    results = results_proxy.fetchmany(size)  # Getting data

    while results:
        from_db = []

        for result in results:
            from_db.append(list(result))

        session_client_db.close()

        raw_data = pd.DataFrame(from_db, columns=client_columns_list)
        primary_key_data = raw_data[primary_key_name]
        raw_data.pop(primary_key_name)

        results = results_proxy.fetchmany(size)  # Getting data

        include_hash_column(
            session_user_db=session_user_db,
            table_user_db=table_user_db,
            primary_key_data=primary_key_data,
            raw_data=raw_data,
        )
