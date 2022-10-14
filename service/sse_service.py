import time

import hashlib
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, insert, select, update

from model import databases_model, model_user_db, schema_client_db


def include_hash_column(tn, classes_db, engine_db, raw_data):
    
    session_db = Session(engine_db['user']) # Section to run sql operation

    id = 1

    for row in range(raw_data.shape[0]):
        record = raw_data[row]
        record = record.copy(order='C')
        hashed_line = hashlib.sha256(record).hexdigest()
        #print(hashed_line)

        stmt = (
            insert(classes_db['user'][f"{tn}"]).
            values(id=id, line_hash=hashed_line)
        )

        session_db.execute(stmt)

        id = id + 1

        print(row)
    
    session_db.commit()
    session_db.close()


def update_hash_column(tn, classes_db, engine_db, raw_data):

    session_db = Session(engine_db['user']) # Section to run sql operation

    id = 1

    for row in range(raw_data.shape[0]):
        record = raw_data[row]
        record = record.copy(order='C')
        hashed_line = hashlib.sha256(record).hexdigest()
        #print(hashed_line)

        stmt = (
            update(classes_db['user'][f"{tn}"]).
            where(classes_db['user'][f"{tn}"].id == id).
            values(line_hash=hashed_line)
        )

        session_db.execute(stmt)

        id = id + 1

        print(row)
    
    session_db.commit()
    session_db.close()


def searchable_encryption(master_key, columns_list, tn, engine_db, classes_db, schemas_db, hash_already_generated):
    index_header = []
    for i in range(1, len(columns_list) + 1):
        index_header.append("index_" + str(i))

    from_db = []
    document_index = []

    session_db = Session(engine_db['client']) # Section to run sql operation

    size = 1000
    statement = select(classes_db['client'][f"{tn}"])
    results_proxy = session_db.execute(statement).scalars() # Proxy to get data on batch
    results = results_proxy.fetchmany(size) # Getting data

    while results:
        #print(results[0].to_list())
        for result in results:
            from_db.append(list(schemas_db['client'][f"{tn}"].dump(result).values()))

        results = results_proxy.fetchmany(size) # Getting data
    
    #print(from_db)
    session_db.close()

    raw_data = pd.DataFrame(from_db, columns=columns_list)
    features = list(raw_data)
    raw_data = raw_data.values

    column_number = [i for i in range(0, len(features)) if features[i] in columns_list]
    
    if hash_already_generated:
        update_hash_column(tn, classes_db, engine_db, raw_data)
    else:
        include_hash_column(tn, classes_db, engine_db, raw_data)

    
def generate_hash(client_db_name, user_db_name):
    engine_db = {}

    engine_db['client'] = create_engine(f"mysql://root:Dd16012018@localhost:3306/ficticio_database")
    engine_db['user'] = create_engine(f"mysql://root:Dd16012018@localhost:3306/{user_db_name}")
    
    table_names = [] #name of the table in database to be encrypted

    classes_db = {} # classes to model database
    classes_db['client'] = {}
    classes_db['user'] = {}

    schemas_db = {} # schemas of models_original_db
    schemas_db['client'] = {}
    schemas_db['user'] = {}
    
    for table_name in engine_db['client'].table_names():
        table_names.append(table_name)
        classes_db['client'][f"{table_name}"] = eval(f"model_client_db.{table_name.capitalize()}")
        schemas_db['client'][f"{table_name}"] = eval(f"schema_client_db.Schema{table_name.capitalize()}()")
        classes_db['user'][f"{table_name}"] = eval(f"model_user_db.{table_name.capitalize()}")
        schemas_db['user'][f"{table_name}"] = eval(f"model_user_db.Schema{table_name.capitalize()}()")
        #print(table_name) 

    master_key_file_name = "./service/masterkey" #password autentication
    master_key = open(master_key_file_name).read()
    if len(master_key) > 16:
        print("the length of master key is larger than 16 bytes, only the first 16 bytes are used")
        master_key = bytes(master_key[:16])

    #keyword_list_file_name = "keywordlist" #name of the columns in database
    #keyword_type_list = open(keyword_list_file_name).read().split(",")

    total_time = 0

    for tn in table_names:

        start_time = time.time()
        columns_list = []
        data_description = classes_db['client'][f"{tn}"].__table__.columns.keys()
        
        # Checking if the hash lines already exist
        session_db = Session(engine_db['user']) # Section to run sql operation
        data_user_per_table = session_db.query(classes_db['user'][f"{tn}"]).all()
        hash_already_generated = False
        
        #If the hashed lines already exist then delete it to generate the hash again
        if data_user_per_table: 
            hash_already_generated = True
        #print(tn)

        for column in data_description:
            columns_list.append(column)
        #print(columns_list)

        searchable_encryption(master_key, columns_list, tn, engine_db, classes_db, schemas_db, hash_already_generated)

        time_cost = time.time() - start_time
        total_time += time_cost

    print(total_time)
    print("Finished")

if __name__ == "__main__":
    generate_hash('ficticio_database', 'UserDB')