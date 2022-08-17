import requests

from app.main.config import app_config
from app.main.config_client import ConfigClient
from app.main.exceptions import ValidationException


def login_api() -> str:

    url = f"{app_config.API_URL}/login"
    body = {
        "email": ConfigClient.USER_EMAIL,
        "password": ConfigClient.USER_PASSWORD,
    }

    response = requests.post(url, json=body)

    if response.status_code != 200:
        raise ValidationException(
            errors={"user": "user_invalid_data"},
            message="Input payload validation failed",
        )

    return f"Bearer {response.json()['token']}"
