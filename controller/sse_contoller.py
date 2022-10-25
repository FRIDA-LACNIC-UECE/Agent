from flask import request, jsonify
from sqlalchemy import create_engine

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE
)
from controller import app
import service.sse_service as sse_service


@app.route('/generate_hash', methods=['POST'])
def generate_hash():
    try:
        src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
            TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
            HOST, PORT, NAME_DATABASE
        )

        src_user_db_path = "{}://{}:{}@{}:{}/{}".format(
            TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
            HOST, PORT, "UserDB"
        )
        
        src_table = request.json.get('table')

        if not src_table:
            # Creating connection with client database
            engine_user_db = create_engine(src_user_db_path)

            # Run hash generator for each client database table
            for table in list(engine_user_db.table_names()):
                print(table)
                sse_service.generate_hash(src_client_db_path, src_user_db_path, table)
        else:
            sse_service.generate_hash(src_client_db_path, src_user_db_path, src_table)

        return jsonify({'message': "hash_generated"}), 201
    except:
        return jsonify({'message': "hash_invalid_data'"}), 400


    