import os

from sqlalchemy_utils import create_database, database_exists

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE
)
from controller import app, db
from service import create_model_service


@app.route('/initialize_database', methods=['GET'])
def initialize_database():

    from model import model_user_db

    os.system("flask db init")
    os.system('flask db migrate -m "Initial migration"')
    os.system('flask db upgrade')

    return {'message': "Database initialized successfully."}, 201