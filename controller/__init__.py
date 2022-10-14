from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_cors import CORS

from config import (
    NAME_DATABASE, TYPE_DATABASE, USER_DATABASE, 
    PASSWORD_DATABASE, HOST, PORT,
)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'{TYPE_DATABASE}://{USER_DATABASE}:{PASSWORD_DATABASE}@{HOST}:{PORT}/{NAME_DATABASE}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

db = SQLAlchemy(app)
ma = Marshmallow(app)
CORS(app)