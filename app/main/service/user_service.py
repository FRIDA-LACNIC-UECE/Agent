import requests

from app.main.config import app_config
from app.main.config_client import ConfigClient
from app.main.exceptions import DefaultException, ValidationException


def login_api(email: str = None, password: str = None) -> tuple[int, str]:
    if email is None or password is None:
        email = ConfigClient.USER_EMAIL
        password = ConfigClient.USER_PASSWORD

    response = requests.post(
        url=f"{app_config.API_URL}/login",
        json={"email": email, "password": password},
    )

    if response.status_code != 200:
        raise DefaultException("user_not_logged_in", code=500)

    id = response.json()["id"]
    token = f"Bearer {response.json()['token']}"

    return (id, token)
