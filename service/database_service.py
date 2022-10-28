import time
from turtle import update
import requests

from sqlalchemy import create_engine, select, inspect, MetaData, Table
from sqlalchemy.orm import Session

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE, BASE_URL
)

from service.user_service import loginApi
from service.sse_service import generate_hash, show_cloud_hash_rows


def checking_changes():
    # Get path of Client DataBase
    src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, NAME_DATABASE
    )

    # Creating connection with user database
    src_user_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, "UserDB"
    )
    engine_user_db = create_engine(src_user_db_path)
    engine_user_db._metadata = MetaData(bind=engine_user_db)
    engine_user_db._metadata.reflect(engine_user_db)  # get columns from existing table
    session_user_db = Session(engine_user_db)

    # Generate rows hash each table
    for table in list(engine_user_db.table_names()):
        print(table)
        generate_hash(src_client_db_path, src_user_db_path, table)

    # Get id cloud database
    url = f'{BASE_URL}/getDatabases'

    response = loginApi()

    if not response:
        return 400

    header = {"Authorization": response}
    response = requests.get(url, headers=header)

    id_db = None
    for database in response.json():
        if (
            database["name_db_type"] == TYPE_DATABASE and 
            database["user"] == USER_DATABASE and
            database["password"] == PASSWORD_DATABASE and
            database["host"] == HOST and
            database["port"] == int(PORT) and
            database["name"] == NAME_DATABASE
        ):
            id_db = database["id"]

    if id_db == None:
        return 404

    token = loginApi()

    if not response:
        return 400

    # Checking
    for table in list(engine_user_db.table_names()):
        page = 0
        per_page = 1000

        # Get columns of table
        client_user_list = []
        insp = inspect(engine_user_db)
        columns_table = insp.get_columns(table)
        for c in columns_table :
            client_user_list.append(str(c['name']))

        # Create engine, reflect existing columns, and create table object for oldTable
        # change this for your source database
        engine_user_db._metadata.tables[table].columns = [
            i for i in engine_user_db._metadata.tables[table].columns if (i.name in ["id", "line_hash"])]
        table_user_db = Table(table, engine_user_db._metadata)

        # Get data in Cloud Database
        results_cloud_data = show_cloud_hash_rows(id_db, table, page, per_page, token)

        # Get data in User Database
        query = session_user_db.query(
            table_user_db
        ).filter(
            table_user_db.c[0] >= (page*per_page), 
            table_user_db.c[0] <= ((page+1)*per_page)
        )

        results_user_data = {}
        results_user_data['primary_key'] = []
        results_user_data['row_hash'] = []
        for row in query:
            results_user_data['primary_key'].append(row[0])
            results_user_data['row_hash'].append(row[1])

        # Trasnform to set
        set_user_hash = set(results_user_data['row_hash'])
        set_cloud_hash = set(results_cloud_data['row_hash'])

        print(f"===== {table} =====")

        diff_ids_user = []
        diff_ids_cloud = []

        # Get data in User Database and Cloud Database
        while len(results_user_data['primary_key']) != 0:
            # Get start id
            start_id = (page*per_page)  

            # Get differences between User Database and Cloud Database
            diff_hashs_user = list(set_user_hash.difference(set_cloud_hash))

            for diff_hash in diff_hashs_user:
                diff_index = results_user_data['row_hash'].index(diff_hash)
                diff_ids_user.append(results_user_data['primary_key'][diff_index])

            diff_hashs_cloud = list(set_cloud_hash.difference(set_user_hash))

            for diff_hash in diff_hashs_cloud:
                diff_index = results_cloud_data['row_hash'].index(diff_hash)
                diff_ids_cloud.append(results_cloud_data['primary_key'][diff_index])

            page += 1

            # Get data in Cloud Database
            results_cloud_data = show_cloud_hash_rows(id_db, table, page, per_page, token)

            # Get data in User Database
            query = session_user_db.query(
                table_user_db
            ).filter(
                table_user_db.c[0] >= (page*per_page), 
                table_user_db.c[0] <= ((page+1)*per_page)
            )

            results_user_data = {}
            results_user_data['primary_key'] = []
            results_user_data['row_hash'] = []
            for row in query:
                results_user_data['primary_key'].append(row[0])
                results_user_data['row_hash'].append(row[1])

            # Trasnform to set
            set_user_hash = set(results_user_data['row_hash'])
            set_cloud_hash = set(results_cloud_data['row_hash'])

        # Get differences
        diff_ids_user = set(diff_ids_user)
        diff_ids_cloud = set(diff_ids_cloud)

        add_ids = diff_ids_user.difference(diff_ids_cloud)
        update_ids = diff_ids_user.intersection(diff_ids_cloud)
        remove_ids = diff_ids_cloud.difference(diff_ids_user)

        print(f"Add ids -> {list(add_ids)}")
        print(f"Update ids -> {list(update_ids)}")
        print(f"Remove ids -> {list(remove_ids)}")

    print("\n\n========= FIM ===========\n\n")

    return


def check_thread():
    # Set time period of task
    seconds_task = 50

    # Running Task
    while True:
        # Checking runtime
        print(checking_changes())
        print()
        time.sleep(seconds_task)