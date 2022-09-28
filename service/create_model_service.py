import os

from sqlalchemy import create_engine


def create_model_client(src_db):
    # Create classes to model the database
    os.system(f"sqlacodegen {src_db} > ./model/model_client_db.py")

    from model import model_client_db

    # Create connection database
    engine_db = create_engine(src_db)

    # Get classes database and create schemas
    classes_db = {}
    file_schema = open('./model/schema_client_db.py', 'w')
    initial_string = ''

    # Import Schema of Marshmallow
    initial_string += "from marshmallow import Schema\n\n"

    for table_name in engine_db.table_names():
        #print(table_name.capitalize())
        classes_db[f"{table_name}"] = eval(f"model_client_db.{table_name.capitalize()}")

        initial_string += f"class Schema{table_name.capitalize()}(Schema):\n"

        initial_string += "\tclass Meta:\n"
        
        initial_string += "\t\tfields = ("

        for field in classes_db[f"{table_name}"].__table__.columns.keys()[0:-1]:
            initial_string += f"'{field}',"

        last_field = classes_db[f"{table_name}"].__table__.columns.keys()[-1]

        initial_string += f"'{last_field}')\n"

        initial_string += "\t\tordered = True\n\n"

    file_schema.write(initial_string)

    return


def create_model_user(src_db_client):

    from model import model_client_db

    # ?
    engine_db = create_engine(src_db_client)

    # Get classes database and create schemas
    classes_db = {}
    file_schema = open('./model/model_user_db.py', 'w')
    initial_string = ''

    # Writting imports
    initial_string += "# coding: utf-8\n"
    initial_string += "from sqlalchemy import Column, Integer, Table, Text\n"
    initial_string += "from sqlalchemy.sql.sqltypes import NullType\n"
    initial_string += "from sqlalchemy.ext.declarative import declarative_base\n"
    initial_string += "from controller import db, ma\n\n\n"

    for table_name in engine_db.table_names():
        
        # Create model user database
        initial_string += f"class {table_name.capitalize()}(db.Model):\n"
        initial_string += f"\t__tablename__ = '{table_name}'\n"
        initial_string += f"\tid = db.Column(db.Integer, primary_key=True)\n"
        initial_string += f"\tline_hash = db.Column(db.Text)\n\n"

        # Create schemas user database
        initial_string += f"class Schema{table_name.capitalize()}(ma.Schema):\n"
        initial_string += f"\tclass Meta:\n"
        initial_string += f"\t\tfields = ('id', 'line_hash')\n"
        initial_string += f"\tordered = True\n\n\n"

    file_schema.write(initial_string)

    return
    
    
if __name__ == '__main__':
    create_model_client(src_db="mysql://root:Dd16012018@localhost:3306/ficticio_database")
    create_model_user(src_db_client="mysql://root:Dd16012018@localhost:3306/ficticio_database")