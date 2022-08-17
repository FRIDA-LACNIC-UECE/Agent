import requests
from sqlalchemy import Column, Integer, MetaData, Table, Text, create_engine, inspect
from sqlalchemy.orm import Session
from sqlalchemy_utils import create_database, database_exists

from config import BASE_URL, SRC_CLIENT_DB_PATH, SRC_USER_DB_PATH
from service.user_service import loginApi


def get_sensitive_columns(id_db, table_name, token):

    url = f"{BASE_URL}/getSensitiveColumns"

    body = {"id_db": id_db, "table": table_name}

    header = {"Authorization": token}

    response = requests.post(url, json=body, headers=header)

    return response.json()


def get_index_column_table_object(table_object, column_name):
    index = 0

    for column in table_object.c:
        if column.name == column_name:
            return index
        index += 1

    return None


def get_columns_database(engine_db, table):
    columns_list = []
    insp = inspect(engine_db)
    columns_table = insp.get_columns(table)

    for c in columns_table:
        columns_list.append(str(c["name"]))

    return columns_list


def get_primary_key(table_name):

    # Create table object of database
    table_object_db, _ = create_table_session(
        src_db_path=SRC_CLIENT_DB_PATH, table=table_name
    )

    return [key.name for key in inspect(table_object_db).primary_key][0]


def get_tables_names(src_db_path):

    try:
        engine_db = create_engine(src_db_path)
    except:
        None

    return list(engine_db.table_names())


def create_table_session(src_db_path, table, columns_list=None):
    # Create engine, reflect existing columns
    engine_db = create_engine(src_db_path)
    engine_db._metadata = MetaData(bind=engine_db)

    # Get columns from existing table
    engine_db._metadata.reflect(engine_db)

    if columns_list == None:
        columns_list = get_columns_database(engine_db, table)

    engine_db._metadata.tables[table].columns = [
        i for i in engine_db._metadata.tables[table].columns if (i.name in columns_list)
    ]

    # Create table object of Client Database
    table_object_db = Table(table, engine_db._metadata)

    # Create session of Client Database to run sql operations
    session_db = Session(engine_db)

    return table_object_db, session_db


def paginate_user_database(session_db, table_db, page, per_page):

    # Get data in User Database
    query = session_db.query(table_db).filter(
        table_db.c[0] >= (page * per_page), table_db.c[0] <= ((page + 1) * per_page)
    )

    results_user_data = {}
    results_user_data["primary_key"] = []
    results_user_data["row_hash"] = []

    for row in query:
        results_user_data["primary_key"].append(row[0])
        results_user_data["row_hash"].append(row[1])

    return results_user_data


def initialize_user_database():
    """
    This initialize user database.

    Parameters
    ----------
        No parameters

    Returns
    -------
    tuple
        (status code, response message).
    """

    # Creating connection with client database
    engine_client_db = create_engine(SRC_CLIENT_DB_PATH)

    if not database_exists(SRC_USER_DB_PATH):
        create_database(SRC_USER_DB_PATH)

    # Creating connection with user database
    engine_user_db = create_engine(SRC_USER_DB_PATH)

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_user_db._metadata = MetaData(bind=engine_user_db)
    engine_user_db._metadata.reflect(engine_user_db)  # get columns from existing table

    for table in list(engine_client_db.table_names()):

        table_user_db = Table(
            table,
            engine_user_db._metadata,
            Column("id", Integer),
            Column("line_hash", Text),
        )

        table_user_db.create()

    return (200, "user_database_started")
