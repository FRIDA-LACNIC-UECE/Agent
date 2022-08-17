import requests
from flask import jsonify

from config import BASE_URL, USER_EMAIL, USER_PASSWORD


def loginApi():
    try:

        url = f"{BASE_URL}/login"
        body = {
            "email": USER_EMAIL,
            "password": USER_PASSWORD,
        }

        response = requests.post(url, json=body)

        if response.status_code == 200:
            return f"Bearer {response.json()['token']}"
        else:
            return None
    except:
        return None
