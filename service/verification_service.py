import time

from config import ID_DB, SRC_CLIENT_DB_PATH, SRC_USER_DB_PATH
from service.cloud_service import (
    delete_cloud_hash_rows,
    insert_cloud_hash_rows,
    process_updates,
    show_cloud_rows_hash,
)
from service.database_service import (
    create_table_session,
    get_tables_names,
    paginate_user_database,
)
from service.sse_service import generate_hash_column
from service.user_service import loginApi


def checking_changes() -> int:
    """
    This function checks client database changes.

    Parameters
    ----------
        No parameters
    Returns
    -------
    int
        status code.
    """

    # Generate rows hash each table
    for table_name in get_tables_names(src_db_path=SRC_USER_DB_PATH):
        print(table_name)
        generate_hash_column(table_name=table_name)

    # Get acess token
    token = loginApi()
    if not token:
        return 400

    # Checking
    for table_name in get_tables_names(src_db_path=SRC_USER_DB_PATH):
        # Start number page
        page = 0

        # Start size page
        per_page = 1000

        # Create table object of User Database and
        # session of User Database to run sql operations
        table_user_db, session_user_db = create_table_session(
            src_db_path=SRC_USER_DB_PATH, table=table_name
        )

        # Get data in Cloud Database
        response_show_cloud_hash_rows = show_cloud_rows_hash(
            id_db=ID_DB,
            table_name=table_name,
            page=page,
            per_page=per_page,
            token=token,
        )
        results_cloud_data = response_show_cloud_hash_rows["result_query"]
        primary_key_value_min_limit = response_show_cloud_hash_rows[
            "primary_key_value_min_limit"
        ]
        primary_key_value_max_limit = response_show_cloud_hash_rows[
            "primary_key_value_max_limit"
        ]

        # Get data in User Database
        results_user_data = paginate_user_database(
            session_user_db, table_user_db, page, per_page
        )

        # Transforme to set
        set_user_hash = set(results_user_data["row_hash"])
        set_cloud_hash = set(results_cloud_data["row_hash"])

        print(f"===== {table_name} =====")

        diff_ids_user = []
        diff_ids_cloud = []

        # Get data in User Database and Cloud Database
        while (page * per_page) < (primary_key_value_max_limit + (per_page * 3)):

            # Get differences between User Database and Cloud Database
            diff_hashs_user = list(set_user_hash.difference(set_cloud_hash))

            for diff_hash in diff_hashs_user:
                diff_index = results_user_data["row_hash"].index(diff_hash)
                diff_ids_user.append(results_user_data["primary_key"][diff_index])

            diff_hashs_cloud = list(set_cloud_hash.difference(set_user_hash))

            for diff_hash in diff_hashs_cloud:
                diff_index = results_cloud_data["row_hash"].index(diff_hash)
                diff_ids_cloud.append(results_cloud_data["primary_key"][diff_index])

            page += 1

            # Get data in Cloud Database
            results_cloud_data = show_cloud_rows_hash(
                id_db=ID_DB,
                table_name=table_name,
                page=page,
                per_page=per_page,
                token=token,
            )["result_query"]

            # Get data in User Database
            results_user_data = paginate_user_database(
                session_user_db, table_user_db, page, per_page
            )

            # Transforme to set
            set_user_hash = set(results_user_data["row_hash"])
            set_cloud_hash = set(results_cloud_data["row_hash"])

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
            insert_cloud_hash_rows(ID_DB, add_ids, table_name)

        # Process updates
        if len(update_ids) != 0:
            process_updates(ID_DB, update_ids, table_name)

        # Delete rows on cloud database
        if len(remove_ids) != 0:
            delete_cloud_hash_rows(ID_DB, remove_ids, table_name)

    session_user_db.commit()
    session_user_db.close()

    print("\n========= FIM ===========\n")

    return 200


def check_thread():
    """
    This function create thread to check client database changes.

    Parameters
    ----------
        No parameters
    Returns
    -------
    int
        status code.
    """

    # Set time period of task
    seconds_task = 50

    # Running Task
    while True:
        # Checking runtime
        checking_changes()
        time.sleep(seconds_task)

    return 200
