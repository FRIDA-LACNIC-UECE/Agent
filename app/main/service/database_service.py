import requests
from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.orm import Session

from app.main.config import app_config
from app.main.config_client import ConfigClient
from app.main.exceptions import ValidationException


def get_sensitive_columns(database_id, table_name, token) -> dict:

    # Send requests
    response = requests.get(
        url=f"{app_config.API_URL}/database/sensitive_columns/{database_id}?table_name={table_name}",
        headers={"Authorization": token},
    )

    # Check status code
    if response.status_code != 200:
        raise ValidationException(
            errors={"database": "database_invalid_data"},
            message="Input payload validation failed",
        )

    return response.json()["sensitive_column_names"]


def get_database_columns(engine, table_name) -> list[str]:
    columns_list = []
    insp = inspect(engine)
    columns_table = insp.get_columns(table_name)

    for c in columns_table:
        columns_list.append(str(c["name"]))

    return columns_list


def get_primary_key(table_name) -> str:

    # Create table object of database
    table_object_db, _ = create_table_session(
        database_url=ConfigClient.CLIENT_DATABASE_URL, table_name=table_name
    )

    return [key.name for key in inspect(table_object_db).primary_key][0]


def create_table_session(database_url, table_name, columns_list=None) -> tuple:
    # Create engine, reflect existing columns
    try:
        engine = create_engine(database_url)
    except:
        return None, None

    engine._metadata = MetaData(bind=engine)

    # Get columns from existing table
    engine._metadata.reflect(engine)

    if columns_list == None:
        columns_list = get_database_columns(engine=engine, table_name=table_name)

    engine._metadata.tables[table_name].columns = [
        i
        for i in engine._metadata.tables[table_name].columns
        if (i.name in columns_list)
    ]

    # Create table object of Client Database
    table_object_db = Table(table_name, engine._metadata)

    # Create session of Client Database to run sql operations
    session_db = Session(engine)

    return table_object_db, session_db


def get_index_column_table_object(table_object, column_name) -> int | None:
    index = 0

    for column in table_object.c:
        if column.name == column_name:
            return index
        index += 1

    return None


def get_tables_names(database_url) -> list[str]:

    try:
        engine_db = create_engine(database_url)
    except:
        return None

    return list(engine_db.table_names())


def paginate_agent_database(session, table_object, page, per_page) -> dict:

    # Get primary key column index in table object
    primary_key_index = get_index_column_table_object(
        table_object=table_object,
        column_name=get_primary_key(table_name=f"{table_object}"),
    )

    # Get data in agent database
    query = session.query(table_object).filter(
        table_object.c[primary_key_index] >= (page * per_page),
        table_object.c[primary_key_index] <= ((page + 1) * per_page),
    )

    results_agent_data = {}
    results_agent_data["primary_key"] = []
    results_agent_data["row_hash"] = []

    for row in query:
        results_agent_data["primary_key"].append(row[0])
        results_agent_data["row_hash"].append(row[1])

    return results_agent_data
