from app.main.exceptions import DefaultException


def write_file_config(
    user_id: int, user_email: str, user_password: str, agent_database: dict
) -> None:
    try:
        with open("./app/main/config_client.py", "w") as config_client:
            config_client.write("class ConfigClient:\n")
            config_client.write(f"\tUSER_ID = {user_id}\n")
            config_client.write(f'\tUSER_EMAIL = "{user_email}"\n')
            config_client.write(f'\tUSER_PASSWORD = "{user_password}"\n')

            config_client.write(f'\tCLIENT_DATABASE_ID = {agent_database["id"]}\n')
            client_database_url = "{}://{}:{}@{}:{}/{}".format(
                agent_database["valid_database"]["dialect"],
                agent_database["username"],
                agent_database["password"],
                agent_database["host"],
                agent_database["port"],
                agent_database["name"],
            )

            config_client.write(f'\tCLIENT_DATABASE_URL = "{client_database_url}"\n')

            agent_database_url = "{}://{}:{}@{}:{}/{}_user_U{}DB{}".format(
                agent_database["valid_database"]["dialect"],
                agent_database["username"],
                agent_database["password"],
                agent_database["host"],
                agent_database["port"],
                agent_database["name"],
                agent_database["user"]["id"],
                agent_database["id"],
            )
            config_client.write(f'\tAGENT_DATABASE_URL = "{agent_database_url}"\n')
    except:
        raise DefaultException("client_config_not_loaded", code=500)

    finally:
        config_client.close()


def agent_start(data: dict[str, str]) -> None:
    database_id = data.get("database_id")
    user_email = data.get("user_email")
    user_password = data.get("user_password")

    user_id, token = login_api(email=user_email, password=user_password)

    client_databases = get_client_databases(token=token)

    agent_database = None
    for database in client_databases:
        if database["id"] == database_id:
            agent_database = database

    if agent_database is None:
        raise DefaultException("client_database_not_found", code=404)

    write_file_config(
        user_id=user_id,
        user_email=user_email,
        user_password=user_password,
        agent_database=agent_database,
    )


from app.main.service.database_service import get_client_databases
from app.main.service.user_service import login_api
