import requests

from config import BASE_URL


def write_file_config(user_email: str, user_password: str, agent_database: dict) -> int:
    """
    This function writes file config.

    Parameters
    ----------
    user_email : str
        User email

    user_password : str
        User password

    agent_database : dict
        Agent Database

    Returns
    -------
    int
        status code.
    """
    f = open("config.py", "w")

    f.write("# Informations Api\n")
    f.write(f'BASE_URL = "{BASE_URL}"\n\n')

    f.write("# Informations client\n")
    f.write(f'USER_EMAIL = "{user_email}"\n')
    f.write(f'USER_PASSWORD = "{user_password}"\n')
    f.write(f'USER_ID = {agent_database["id_user"]}\n\n')

    f.write("# Informations client database\n")
    f.write(f'ID_DB = {agent_database["id"]}\n')
    src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
        agent_database["name_db_type"],
        agent_database["user"],
        agent_database["password"],
        agent_database["host"],
        agent_database["port"],
        agent_database["name"],
    )
    f.write(f'SRC_CLIENT_DB_PATH = "{src_client_db_path}"\n\n')

    f.write("# Informations user database\n")
    src_user_db_path = "{}://{}:{}@{}:{}/{}_user_U{}DB{}".format(
        agent_database["name_db_type"],
        agent_database["user"],
        agent_database["password"],
        agent_database["host"],
        agent_database["port"],
        agent_database["name"],
        agent_database["id_user"],
        agent_database["id"],
    )
    f.write(f'SRC_USER_DB_PATH = "{src_user_db_path}"\n')

    f.close()


def agent_start(user_email: str, user_password: str, id_db: int) -> tuple[int, str]:
    """
    This function star agent

    Parameters
    ----------
    user_email : str
        User email

    user_password : str
        User password

    id_db : int
        Database ID

    Returns
    -------
    tuple
        (status code, response message).
    """

    # Login User
    url = f"{BASE_URL}/login"
    body = {
        "email": user_email,
        "password": user_password,
    }
    response = requests.post(url, json=body)
    if response.status_code != 200:
        return (400, "user_invalid_data")

    # Request getDatabases
    url = f"{BASE_URL}/getDatabases"
    header = {"Authorization": f"Bearer {response.json()['token']}"}
    response = requests.get(url, headers=header)
    if response.status_code != 200:
        return (400, "user_invalid_data")

    # Get user databases
    databases = response.json()

    agent_database = None
    for database in databases:
        if database["id"] == id_db:
            agent_database = database

    if not agent_database:
        return (400, "database_invalid_data")

    # Write file config
    write_file_config(
        user_email=user_email,
        user_password=user_password,
        agent_database=agent_database,
    )

    return (200, "agent_started")
