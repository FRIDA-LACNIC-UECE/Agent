import imp
import time
import threading
from datetime import datetime

from flask import jsonify
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text
from sqlalchemy_utils import create_database, database_exists

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE
)
from controller import app, db
from service.database_service import check_thread


@app.route('/loginApi', methods=['POST'])
def loginApi():
    try:
        return jsonify({'message': "database_initialized"}), 201
    except:
        return jsonify({'message': 'database_invalid_data'}), 400