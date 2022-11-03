import requests

from sqlalchemy import create_engine, select, inspect, MetaData, Table
from sqlalchemy.orm import Session

from config import BASE_URL


def get_sensitive_columns(id_db, token):
    
    url = f'{BASE_URL}/getColumnsToAnonymize'
    
    body = {
        "id_db": id_db
    }

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


def get_columns_database(engine_db, table):
    columns_list = []
    insp = inspect(engine_db)
    columns_table = insp.get_columns(table)

    for c in columns_table :
        columns_list.append(str(c['name']))

    return columns_list


def paginate_user_database(session_db, table_db, page, per_page):
    # Get data in User Database
    query = session_db.query(
        table_db
    ).filter(
        table_db.c[0] >= (page*per_page), 
        table_db.c[0] <= ((page+1)*per_page)
    )

    results_user_data = {}
    results_user_data['primary_key'] = []
    results_user_data['row_hash'] = []

    for row in query:
        results_user_data['primary_key'].append(row[0])
        results_user_data['row_hash'].append(row[1])
    
    return results_user_data