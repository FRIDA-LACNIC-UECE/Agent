from flask import jsonify

from controller import app
from service.database_service import initialize_user_database


@app.route("/initializeDatabase", methods=["GET"])
def initializeDatabase():
    try:
        status_code, response_message = initialize_user_database()

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "database_invalid_data"}), 400
