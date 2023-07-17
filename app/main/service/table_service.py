import requests
from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.orm import Session
from app.main.config import app_config
from app.main.config_client import ConfigClient
from app.main.exceptions import DefaultException


class TableConnection:
    def __init__(self, engine, session, table):
        self.engine = engine
        self.session = session
        self.table_name = table.name
        self.table = table

    def get_column(self, column_name: str):
        for column in self.table.c:
            if column.name == column_name:
                return column
        return None

    def get_primary_key_name(self):
        return [key.name for key in inspect(self.table).primary_key][0]

    def close(self):
        self.session.close()
        self.engine.dispose()


def get_table(database_id: int, table_name: str, token: str) -> dict:
    response = requests.get(
        url=f"{app_config.API_URL}/database/{database_id}/table/{table_name}",
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise DefaultException("client_database_data_not_loaded", code=500)

    return response.json()


def create_table_connection(
    database_url: str, table_name: str, columns_list: list[str] = None
) -> TableConnection:
    try:
        engine = create_engine(database_url)
        engine._metadata = MetaData(bind=engine)
        engine._metadata.reflect(engine)

        if columns_list == None:
            columns_list = get_database_columns(engine=engine, table_name=table_name)

        engine._metadata.tables[table_name].columns = [
            i
            for i in engine._metadata.tables[table_name].columns
            if (i.name in columns_list)
        ]

        table = Table(table_name, engine._metadata)
        session = Session(engine)

        session.get_bind()

        return TableConnection(engine=engine, session=session, table=table)
    except:
        raise DefaultException("internal_error_accessing_database", code=500)


def get_sensitive_columns(database_id: int, table_name: str, token: str) -> dict:
    table = get_table(database_id=database_id, table_name=table_name, token=token)

    response = requests.get(
        url=f"{app_config.API_URL}/database/{database_id}/table/{table['id']}/sensitive_columns",
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise DefaultException("client_database_data_not_loaded", code=500)

    return response.json()["sensitive_column_names"]


def get_database_columns(engine: any, table_name: str) -> list[str]:
    insp = inspect(engine)
    columns_table = insp.get_columns(table_name)

    columns_list = []
    for c in columns_table:
        columns_list.append(str(c["name"]))

    return columns_list


def get_primary_key(table_name) -> str:
    table_connection = create_table_connection(
        database_url=ConfigClient.CLIENT_DATABASE_URL, table_name=table_name
    )

    return [key.name for key in inspect(table_connection.table).primary_key][0]
