import requests

from app.main.config import app_config
from app.main.exceptions import DefaultException, ValidationException


def write_file_config(
    user_id: int, user_email: str, user_password: str, agent_database: dict
) -> int:
    with open("./app/main/config_client.py", "w") as config_client:
        config_client.write("class ConfigClient:\n")
        config_client.write(f"\tUSER_ID = {user_id}\n")
        config_client.write(f'\tUSER_EMAIL = "{user_email}"\n')
        config_client.write(f'\tUSER_PASSWORD = "{user_password}"\n')

        config_client.write(f'\tCLIENT_DATABASE_ID = {agent_database["id"]}\n')
        client_database_url = "mysql://{}:{}@{}:{}/{}".format(
            agent_database["username"],
            agent_database["password"],
            agent_database["host"],
            agent_database["port"],
            agent_database["name"],
        )

        config_client.write(f'\tCLIENT_DATABASE_URL = "{client_database_url}"\n')

        agent_database_url = "mysql://{}:{}@{}:{}/{}_user_U{}DB{}".format(
            agent_database["username"],
            agent_database["password"],
            agent_database["host"],
            agent_database["port"],
            agent_database["name"],
            agent_database["user_id"],
            agent_database["id"],
        )
        config_client.write(f'\tAGENT_DATABASE_URL = "{agent_database_url}"\n')

        config_client.close()


def agent_start(data: dict[str, str]) -> None:

    database_id = data.get("database_id")
    user_email = data.get("user_email")
    user_password = data.get("user_password")

    # Login User
    response_login_user = requests.post(
        url=f"{app_config.API_URL}/login",
        json={
            "email": user_email,
            "password": user_password,
        },
    )

    if response_login_user.status_code != 200:
        raise ValidationException(
            errors={"user": "user_invalid_data"},
            message="Input payload validation failed",
        )

    # Get databases of user
    response_databases = requests.get(
        url=f"{app_config.API_URL}/database",
        headers={"Authorization": f"Bearer {response_login_user.json()['token']}"},
    )

    if response_databases.status_code != 200:
        raise ValidationException(
            errors={"user": "user_invalid_data"},
            message="Input payload validation failed",
        )

    # Get client database id to agent
    databases = response_databases.json()["items"]

    agent_database = None
    for database in databases:

        if database["id"] == database_id:
            agent_database = database

    if not agent_database:
        raise DefaultException("database_not_found", code=404)

    # Write file config
    write_file_config(
        user_id=response_login_user.json()["id"],
        user_email=user_email,
        user_password=user_password,
        agent_database=agent_database,
    )
