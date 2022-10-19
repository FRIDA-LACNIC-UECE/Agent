from flask import request

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE
)
from controller import app


@app.route('/generate_hash', methods=['POST'])
def generate_hash():
    import service.sse_service as sse_service
    from model import model_user_db

    src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, NAME_DATABASE
    )

    src_user_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, "UserDB"
    )
    
    src_table = request.json.get('table')

    sse_service.generate_hash(src_client_db_path, src_user_db_path, src_table)

    return {'message': "Hash generated successfully."}, 201