import requests

from sqlalchemy import MetaData, Table, create_engine
from app.main.config import app_config
from app.main.exceptions import DefaultException
from sqlalchemy import Column, Integer, MetaData, Table, Text, create_engine
from sqlalchemy_utils import create_database, database_exists, drop_database
from app.main.config_client import ConfigClient

from app.main.service.table_service import get_primary_key, TableConnection

from app.main.exceptions import DefaultException


def agent_database_start() -> None:
    try:
        # Creating connection with client database
        engine_client_db = create_engine(ConfigClient.CLIENT_DATABASE_URL)

        if database_exists(url=ConfigClient.AGENT_DATABASE_URL):
            drop_database(url=ConfigClient.AGENT_DATABASE_URL)
        create_database(url=ConfigClient.AGENT_DATABASE_URL)

        # Creating connection with agent database
        engine_agent_db = create_engine(ConfigClient.AGENT_DATABASE_URL)

        # Create engine, reflect existing columns, and create table object for oldTable
        # change this for your source database
        engine_agent_db._metadata = MetaData(bind=engine_agent_db)
        engine_agent_db._metadata.reflect(
            engine_agent_db
        )  # get columns from existing table

        for table in list(engine_client_db.table_names()):
            table_agent_db = Table(
                table,
                engine_agent_db._metadata,
                Column("primary_key", Integer),
                Column("line_hash", Text),
            )

            table_agent_db.create()

    except:
        raise DefaultException("agent_database_not_started", code=500)

    finally:
        engine_client_db.dispose()
        engine_agent_db.dispose()


def get_client_databases(token: str) -> list[dict]:
    response = requests.get(
        url=f"{app_config.API_URL}/database",
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise DefaultException("client_databases_not_loaded", code=500)

    return response.json()["items"]


def get_tables_names(database_url: str) -> list[str]:
    try:
        engine_db = create_engine(database_url)
    except:
        return None

    return list(engine_db.table_names())


def paginate_agent_database(
    table_connection: TableConnection, page: int, per_page: int
) -> dict[str, any]:
    primary_key_name = get_primary_key(table_name=table_connection.table_name)

    query = table_connection.session.query(table_connection.table).filter(
        table_connection.get_column(column_name=primary_key_name) >= (page * per_page),
        table_connection.get_column(column_name=primary_key_name)
        <= ((page + 1) * per_page),
    )

    results_agent_data = {}
    results_agent_data["primary_key"] = []
    results_agent_data["row_hash"] = []

    for row in query:
        results_agent_data["primary_key"].append(row[0])
        results_agent_data["row_hash"].append(row[1])

    return results_agent_data


def paginate_agent_database(
    table_connection: TableConnection, page: int, per_page: int
) -> dict[str, any]:
    primary_key_name = get_primary_key(table_name=table_connection.table_name)

    query = table_connection.session.query(table_connection.table).filter(
        table_connection.get_column(column_name="primary_key") >= (page * per_page),
        table_connection.get_column(column_name="primary_key")
        <= ((page + 1) * per_page),
    )

    results_user_data = {}
    results_user_data["primary_key"] = []
    results_user_data["row_hash"] = []

    for row in query:
        results_user_data["primary_key"].append(row[0])
        results_user_data["row_hash"].append(row[1])

    return results_user_data
