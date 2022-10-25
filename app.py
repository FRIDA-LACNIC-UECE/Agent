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
    app, db, database_controller, sse_contoller
)


Migrate(app, db)

@app.route('/')
def index():
    return {'message': "Agent is running."}, 200

if __name__ == "__main__":
    app.run(debug=True, port=3000)