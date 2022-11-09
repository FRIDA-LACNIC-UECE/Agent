import time

from sqlalchemy import create_engine, select, inspect, MetaData, Table, func
from sqlalchemy.orm import Session


from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE, BASE_URL, PRIMARY_KEY
)
from service.cloud_service import (
    get_id_cloud_database, show_cloud_hash_rows,
    insert_cloud_hash_rows, delete_cloud_hash_rows
)
from service.database_service import paginate_user_database, get_index_column_table_object
from service.sse_service import generate_hash_column
from service.user_service import loginApi


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
        generate_hash_column(src_client_db_path, src_user_db_path, table)

    # Get id cloud database 
    id_db, state_code_id_db = get_id_cloud_database()
    if not id_db:
        return 400
    
    # Get acess token
    token = loginApi()
    if not token:
        return 400

    # Checking
    for table in list(engine_user_db.table_names()):
        # Start number page
        page = 0

        # Start size page
        per_page = 1000

        # Create engine, reflect existing columns, and create table object of table_user_db
        engine_user_db._metadata.tables[table].columns = [
            i for i in engine_user_db._metadata.tables[table].columns 
            if (i.name in [PRIMARY_KEY, "line_hash"])
        ]
        table_user_db = Table(table, engine_user_db._metadata)

        # Index of primary key column
        index_primary = get_index_column_table_object(table_user_db, PRIMARY_KEY)

        # Get data in Cloud Database
        response_show_cloud_hash_rows = show_cloud_hash_rows(
            id_db, table, page, per_page, token
        )
        results_cloud_data = response_show_cloud_hash_rows['result_query']
        primary_key_value_min_limit = response_show_cloud_hash_rows['primary_key_value_min_limit']
        primary_key_value_max_limit = response_show_cloud_hash_rows['primary_key_value_max_limit']

        # Get data in User Database
        results_user_data = paginate_user_database(
            session_user_db, table_user_db, page, per_page
        )

        # Transforme to set
        set_user_hash = set(results_user_data['row_hash'])
        set_cloud_hash = set(results_cloud_data['row_hash'])

        print(f"===== {table} =====")

        diff_ids_user = []
        diff_ids_cloud = []

        # Get data in User Database and Cloud Database
        while (page * per_page) < (primary_key_value_max_limit + (per_page * 3)):

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
            results_cloud_data = show_cloud_hash_rows(
                id_db, table, page, per_page, token
            )['result_query']

            # Get data in User Database
            results_user_data = paginate_user_database(
                session_user_db, table_user_db, page, per_page
            )

            # Transforme to set
            set_user_hash = set(results_user_data['row_hash'])
            set_cloud_hash = set(results_cloud_data['row_hash'])

        # Get differences between user database and cloud database
        diff_ids_user = set(diff_ids_user)
        diff_ids_cloud = set(diff_ids_cloud)

        # Get differences (add, update, remove)
        add_ids = list(diff_ids_user.difference(diff_ids_cloud))
        update_ids = list(diff_ids_user.intersection(diff_ids_cloud))
        remove_ids = list(diff_ids_cloud.difference(diff_ids_user))

        print(f"Add ids -> {add_ids}")
        print(f"Update ids -> {update_ids}")
        print(f"Remove ids -> {remove_ids}")

        # Insert news rows on cloud database
        if len(add_ids) != 0:
            insert_cloud_hash_rows(id_db, add_ids, table)

        # Delete rows on cloud database
        if len(remove_ids) != 0:
            delete_cloud_hash_rows(id_db, remove_ids, table)

    session_user_db.commit()
    session_user_db.close()

    print("\n\n========= FIM ===========\n\n")

    return


def check_thread():
    # Set time period of task
    seconds_task = 50

    # Running Task
    while True:
        # Checking runtime
        checking_changes()
        time.sleep(seconds_task)