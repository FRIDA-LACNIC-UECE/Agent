import datetime
import time
import requests
from json import JSONEncoder
from app.main.config import app_config
from app.main.config_client import ConfigClient
from app.main.exceptions import DefaultException


class DateTimeEncoder(JSONEncoder):
    """
    This class encodes date time to json format.
    """

    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, (str)):
            return obj.encode("utf-8")


def process_inserts(database_id: int, primary_key_list: list, table_name: str):
    # Get acess token
    _, token = login_api()

    # Get table
    table = get_table(database_id=database_id, table_name=table_name, token=token)

    # Get sensitive columns names of Client Database
    sensitive_columns = get_sensitive_columns(
        database_id=database_id, table_name=table_name, token=token
    )

    # Get primary key name
    primary_key_name = get_primary_key(table_name=table_name)

    # Add primary key in sensitive columns only to query
    sensitive_columns.append(get_primary_key(table_name=table_name))

    # Create table connection of Client Database
    client_table_connection = create_table_connection(
        database_url=ConfigClient.CLIENT_DATABASE_URL,
        table_name=table_name,
        columns_list=sensitive_columns,
    )

    # Get news rows to encrypted and send Cloud Database
    news_rows_client_db = []
    for primary_key in primary_key_list:
        result = (
            client_table_connection.session.query(client_table_connection.table)
            .filter(
                client_table_connection.get_column(column_name=primary_key_name)
                == primary_key
            )
            .first()
        )

        news_rows_client_db.append(result._asdict())

    # Get date type columns on news rows of Client Database
    first_row = news_rows_client_db[0]

    data_type_keys = []
    for key in first_row.keys():
        type_data = str(type(first_row[key]).__name__)
        if type_data == "date":
            data_type_keys.append(key)

    # Convert from date type to string
    for row in news_rows_client_db:
        for key in data_type_keys:
            row[key] = row[key].strftime("%Y-%m-%d")

    # Encrypt new rows and send Cloud Database
    encrypt_new_rows(
        database_id=database_id,
        table_id=table["id"],
        news_rows_client_db=news_rows_client_db,
        token=token,
    )
    print("--- Encriptou as novas linhas ---")

    # Anonymization new row
    anonymize_new_rows(
        database_id=database_id,
        table_id=table["id"],
        news_rows_client_db=news_rows_client_db,
        token=token,
    )
    client_table_connection.session.commit()
    print("--- Anonimizou as novas linhas ---")

    # Get anonymized news rows to generate their hash
    anonymized_news_rows = []
    for primary_key in primary_key_list:
        result = (
            client_table_connection.session.query(client_table_connection.table)
            .filter(
                client_table_connection.get_column(column_name=primary_key_name)
                == primary_key
            )
            .first()
        )

        anonymized_news_rows.append(list(result))
    print(f"anonymized_news_rows = {anonymized_news_rows}")

    # Generate hash of anonymized new rows
    generate_hash_rows(
        table_name=table_name,
        result_query=anonymized_news_rows,
    )
    print("--- Gerou hashs das novas linhas ---")

    # Create connection of Agent Database
    agent_table_connection = create_table_connection(
        database_url=ConfigClient.AGENT_DATABASE_URL,
        table_name=table_name,
        columns_list=["primary_key", "line_hash"],
    )
    agent_table_connection.session.commit()

    # Get hashs of anonymized news rows to insert Cloud Database
    agent_rows_to_insert = []
    for primary_key in primary_key_list:
        result = (
            agent_table_connection.session.query(agent_table_connection.table)
            .where(
                agent_table_connection.get_column(column_name="primary_key")
                == primary_key
            )
            .first()
        )

        agent_rows_to_insert.append(result._asdict())
    print(f"agent_rows_to_insert = {agent_rows_to_insert}")

    # Include hash rows in Cloud Database
    response = requests.post(
        url=f"{app_config.API_URL}/agent/process_inserts/database/{database_id}/table/{table['id']}",
        json={"hash_rows": agent_rows_to_insert},
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise DefaultException("inserts_not_processed", code=500)
    print("--- Incluiu hashs das novas linhas ---")


def process_updates(database_id: int, table_name: str, primary_key_list: list):
    _, token = login_api()

    table = get_table(database_id=database_id, table_name=table_name, token=token)

    response = requests.post(
        url=f"{app_config.API_URL}/agent/process_updates/database/{database_id}/table/{table['id']}",
        json={"primary_key_list": primary_key_list},
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise DefaultException("updates_not_processed", code=500)


def process_deletions(database_id: int, table_name: str, primary_key_list: list):
    _, token = login_api()

    table = get_table(database_id=database_id, table_name=table_name, token=token)

    response = requests.post(
        url=f"{app_config.API_URL}/agent/process_deletions/database/{database_id}/table/{table['id']}",
        json={"table_name": table_name, "primary_key_list": primary_key_list},
        headers={"Authorization": token},
    )

    if response.status_code != 200:
        raise DefaultException("deletions_not_processed", code=500)


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

    print("\n===== Initializing verification:")

    # Generate rows hash each table
    for table_name in get_tables_names(database_url=ConfigClient.CLIENT_DATABASE_URL):
        print(f"\n===== {table_name} =====")

        try:
            generate_hash_column(table_name=table_name)
        except:
            print("table_not_ready_verification")
            print("\nFinalizing Verification =====\n")
            return

        # Get acess token
        _, token = login_api()

        # Start number page
        page = 0

        # Start size page
        per_page = 100

        # Create table connection Agent Database
        agent_table_connection = create_table_connection(
            database_url=ConfigClient.AGENT_DATABASE_URL, table_name=table_name
        )

        # Get data in Cloud Database
        response_show_cloud_hash_rows = show_cloud_rows_hash(
            database_id=ConfigClient.CLIENT_DATABASE_ID,
            table_name=table_name,
            page=page,
            per_page=per_page,
            token=token,
        )
        results_cloud_data = response_show_cloud_hash_rows["row_hash_list"][0]
        primary_key_value_min_limit = response_show_cloud_hash_rows[
            "primary_key_value_min_limit"
        ]
        primary_key_value_max_limit = response_show_cloud_hash_rows[
            "primary_key_value_max_limit"
        ]

        # Get data in Agent Database
        results_agent_data = paginate_agent_database(
            table_connection=agent_table_connection, page=page, per_page=per_page
        )

        # Transforme to set
        set_agent_hash = set(results_agent_data["row_hash"])
        set_cloud_hash = set(results_cloud_data["row_hash"])

        diff_ids_agent = []
        diff_ids_cloud = []

        # Get data in Agent Database and Cloud Database
        while (page * per_page) <= (primary_key_value_max_limit):
            # Get differences between Agent Database and Cloud Database
            diff_hashs_agent = list(set_agent_hash.difference(set_cloud_hash))

            for diff_hash in diff_hashs_agent:
                diff_index = results_agent_data["row_hash"].index(diff_hash)
                diff_ids_agent.append(results_agent_data["primary_key"][diff_index])

            diff_hashs_cloud = list(set_cloud_hash.difference(set_agent_hash))

            for diff_hash in diff_hashs_cloud:
                diff_index = results_cloud_data["row_hash"].index(diff_hash)
                diff_ids_cloud.append(results_cloud_data["primary_key"][diff_index])

            page += 1

            # Get data in Cloud Database
            results_cloud_data = show_cloud_rows_hash(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                page=page,
                per_page=per_page,
                token=token,
            )["row_hash_list"][0]

            # Get data in Agent Database
            results_agent_data = paginate_agent_database(
                table_connection=agent_table_connection, page=page, per_page=per_page
            )

            # Transforme to set
            set_agent_hash = set(results_agent_data["row_hash"])
            set_cloud_hash = set(results_cloud_data["row_hash"])

        # Get differences between Agent database and cloud database
        diff_ids_agent = set(diff_ids_agent)
        diff_ids_cloud = set(diff_ids_cloud)

        # Get differences (add, update, remove)
        insert_ids = list(diff_ids_agent.difference(diff_ids_cloud))
        update_ids = list(diff_ids_agent.intersection(diff_ids_cloud))
        delete_ids = list(diff_ids_cloud.difference(diff_ids_agent))

        print(f"Insert IDs -> {insert_ids}")
        print(f"Update IDs -> {update_ids}")
        print(f"Delete IDs -> {delete_ids}")

        # Insert news rows on cloud database
        if len(insert_ids) != 0:
            inserts_log(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=insert_ids,
            )
            process_inserts(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=insert_ids,
            )

        # Process updates
        if len(update_ids) != 0:
            process_updates(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=update_ids,
            )

        # Delete rows on cloud database
        if len(delete_ids) != 0:
            deletions_log(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=delete_ids,
            )
            process_deletions(
                database_id=ConfigClient.CLIENT_DATABASE_ID,
                table_name=table_name,
                primary_key_list=delete_ids,
            )

    agent_table_connection.commit()
    agent_table_connection.close()

    return 200


def create_verification_thread():
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
    seconds_task = 30

    # Running Task
    while True:
        # Checking runtime
        checking_changes()
        time.sleep(seconds_task)


from app.main.service.sql_log_service import deletions_log, inserts_log
from app.main.service.sse_service import (
    generate_hash_column,
    generate_hash_rows,
    show_cloud_rows_hash,
)
from app.main.service.user_service import login_api
from app.main.service.database_service import paginate_agent_database, get_tables_names
from app.main.service.table_service import (
    get_sensitive_columns,
    get_primary_key,
    create_table_connection,
    get_table,
)
from app.main.service.anonymization_service import anonymize_new_rows
from app.main.service.encryption_service import encrypt_new_rows
