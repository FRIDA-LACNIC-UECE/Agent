from flask import jsonify
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text
from sqlalchemy_utils import create_database, database_exists

from config import (
    TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, 
    HOST, PORT, NAME_DATABASE
)
from controller import app
from service.database_service import create_table_session, get_index_column_table_object


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


@app.route('/testRoute', methods=['GET'])
def testRoute():
    # Get path of Client DataBase
    src_client_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE,
        HOST, PORT, NAME_DATABASE
    )

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_client_db_path, "nivel1"
    )

    get_index_column_table_object(table_client_db, "id")

    return {"Message": "Deu bom!"}