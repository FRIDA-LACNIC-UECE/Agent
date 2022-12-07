import requests
from flask import jsonify

from config import BASE_URL, USER_EMAIL, USER_PASSWORD
from controller import app


@app.route("/loginApi", methods=["GET"])
def loginApi():
    try:

        url = f"{BASE_URL}/login"
        body = {
            "email": USER_EMAIL,
            "password": USER_PASSWORD,
        }

        response = requests.post(url, json=body)

        return jsonify({"token": f"Bearer {response.json()['token']}"}), 200
    except:
        return jsonify({"message": "user_invalid_data"}), 400
