from flask import jsonify, request
from sqlalchemy import create_engine

from config import SRC_CLIENT_DB_PATH, SRC_USER_DB_PATH
from controller import app
from service.sse_service import generate_hash_column


@app.route("/generateHashColumn", methods=["POST"])
def generateHash():
    src_table = request.json.get("table")

    if not src_table:
        # Creating connection with client database
        engine_user_db = create_engine(SRC_CLIENT_DB_PATH)

        # Run hash generator for each client database table
        for table in list(engine_user_db.table_names()):
            generate_hash_column(SRC_CLIENT_DB_PATH, SRC_USER_DB_PATH, table)
    else:
        generate_hash_column(SRC_CLIENT_DB_PATH, SRC_USER_DB_PATH, src_table)

    return jsonify({"message": "hash_generated"}), 201
    # except:
    # return jsonify({'message': "hash_invalid_data'"}), 400
