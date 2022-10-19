import os

from flask import request
from flask_migrate import Migrate
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import Session

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE
)
from controller import (
    app, db, create_model_controller, database_controller, sse_contoller
)


Migrate(app, db)

@app.route('/')
def index():
    return {'message': "Agent is running."}, 200

if __name__ == "__main__":
    
    src_user_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, "UserDB"
    )
    
    if not database_exists(src_user_db_path):
        create_database(src_user_db_path)

    app.run(debug=True, port=3000)