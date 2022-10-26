import os
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
from service.database_service import check_thread, checking_changes


@app.route('/initializeDatabase', methods=['GET'])
def initializeDatabase():
    try:
        src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
            TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
            HOST, PORT, NAME_DATABASE
        )

        # Creating connection with client database
        engine_client_db = create_engine(src_client_db_path)

        src_user_db_path = "{}://{}:{}@{}:{}/{}".format(
            TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
            HOST, PORT, f"UserDB"
        )
        
        if not database_exists(src_user_db_path):
            create_database(src_user_db_path)

        #Creating connection with user database
        engine_user_db = create_engine(src_user_db_path)

        # Create engine, reflect existing columns, and create table object for oldTable
        # change this for your source database
        engine_user_db._metadata = MetaData(bind=engine_user_db)
        engine_user_db._metadata.reflect(engine_user_db)  # get columns from existing table

        for table in list(engine_client_db.table_names()):

            table_user_db = Table(
                table, engine_user_db._metadata,
                Column("id", Integer),
                Column("line_hash", Text)
            )
        
            table_user_db.create()
    
        return jsonify({'message': "database_initialized"}), 201
    except:
        return jsonify({'message': 'database_invalid_data'}), 400
    

@app.route('/initializeVerification', methods=['GET'])
def initializeVerification():
    # Run task on thread
    thread = threading.Thread(target=check_thread)
    thread.start()
    return jsonify({'message': "verification_initialized"}), 201


    