import time
import requests

from sqlalchemy import create_engine, select
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
        print(database["password"])
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

        results = show_cloud_hash_rows(id_db, table, page, token)
        
        while len(results) != 0:
            print(results)
            page += 1
            results = show_cloud_hash_rows(id_db, table, page, token)

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