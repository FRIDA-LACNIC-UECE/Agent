from flask import jsonify, request

from controller import app
from service.agent_service import agent_start


@app.route("/startAgent", methods=["POST"])
def startAgent():
    user_email = request.json.get("email")
    user_password = request.json.get("password")
    id_db = request.json.get("id_db")

    status_code, response_message = agent_start(
        user_email=user_email, user_password=user_password, id_db=id_db
    )

    return jsonify({"message": response_message}), status_code
