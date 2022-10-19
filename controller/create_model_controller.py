from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE
)

from controller import app, db

from service import create_model_service


@app.route('/create_model_client', methods=['GET'])
def create_model_client():
    src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, NAME_DATABASE
    )

    create_model_service.create_model_client(src_db_client=src_client_db_path)
    create_model_service.create_model_user(src_db_client=src_client_db_path)
    
    return {'message': "Client and User Models created successfully."}, 201