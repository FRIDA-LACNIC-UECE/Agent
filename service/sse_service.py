import time

import hashlib
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, inspect, update, insert
from sqlalchemy.orm import Session


def include_hash_column(engine_client_db, engine_user_db, table_client_db, table_user_db, src_table, raw_data):
    
    session_user_db = Session(engine_user_db) # Section to run sql operation

    for row in range(raw_data.shape[0]):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(
            new_record.encode('utf-8')
        ).hexdigest()

        stmt = (
            insert(table_user_db).
            values(id=raw_data.iloc[row]['id'], line_hash=hashed_line)
        )

        session_user_db.execute(stmt)
    
    session_user_db.commit()
    session_user_db.close()


def update_hash_column(engine_client_db, engine_user_db, table_client_db, table_user_db, src_table, raw_data):

    session_user_db = Session(engine_user_db) # Section to run sql operation

    for row in range(raw_data.shape[0]):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(
            new_record.encode('utf-8')
        ).hexdigest()

        stmt = (
            update(table_user_db).
            where(table_user_db.c[0] == raw_data.iloc[row]['id']).
            values(line_hash=hashed_line)
        )

        session_user_db.execute(stmt)

        print(row)
    
    session_user_db.commit()
    session_user_db.close()


def searchable_encryption(engine_client_db, engine_user_db, src_table, client_columns_list, table_client_db, table_user_db, master_key, hash_already_generated):
    index_header = []
    for i in range(1, len(client_columns_list) + 1):
        index_header.append("index_" + str(i))

    from_db = []
    document_index = []

    session_client_db = Session(engine_client_db) # Section to run sql operation

    size = 1000
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(statement) # Proxy to get data on batch
    results = results_proxy.fetchmany(size) # Getting data

    while results:
        from_db = []

        for result in results:
            from_db.append(list(result))
            print(result)

        session_client_db.close()

        raw_data = pd.DataFrame(from_db, columns=client_columns_list)
        features = list(raw_data)
        #raw_data = raw_data.values

        column_number = [i for i in range(0, len(features)) if features[i] in client_columns_list]
        
        if hash_already_generated:
            update_hash_column(engine_client_db, engine_user_db, table_client_db, table_user_db, src_table, raw_data)
        else:
            include_hash_column(engine_client_db, engine_user_db, table_client_db, table_user_db, src_table, raw_data)

        results = results_proxy.fetchmany(size) # Getting data
    
    
def generate_hash(src_client_db_path, src_user_db_path, src_table):
    # Creating connection with client database
    engine_client_db = create_engine(src_client_db_path)
    session_client_db = Session(engine_client_db)

    # Get columns of table
    client_columns_list = []
    insp = inspect(engine_client_db)
    columns_table = insp.get_columns(src_table)

    for c in columns_table :
        client_columns_list.append(str(c['name']))
    #print(client_columns_list)

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_client_db._metadata = MetaData(bind=engine_client_db)
    engine_client_db._metadata.reflect(engine_client_db)  # get columns from existing table
    engine_client_db._metadata.tables[src_table].columns = [
        i for i in engine_client_db._metadata.tables[src_table].columns if (i.name in client_columns_list)]
    table_client_db = Table(src_table, engine_client_db._metadata)

    # Creating connection with user database
    engine_user_db = create_engine(src_user_db_path)
    session_user_db = Session(engine_user_db)

    # Get columns of table
    client_user_list = []
    insp = inspect(engine_user_db)
    columns_table = insp.get_columns(src_table)

    for c in columns_table :
        client_user_list.append(str(c['name']))

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_user_db._metadata = MetaData(bind=engine_user_db)
    engine_user_db._metadata.reflect(engine_user_db)  # get columns from existing table
    engine_user_db._metadata.tables[src_table].columns = [
        i for i in engine_user_db._metadata.tables[src_table].columns if (i.name in client_user_list)]
    table_user_db = Table(src_table, engine_user_db._metadata)

    master_key_file_name = "./service/masterkey" #password autentication
    master_key = open(master_key_file_name).read()
    if len(master_key) > 16:
        print("the length of master key is larger than 16 bytes, only the first 16 bytes are used")
        master_key = bytes(master_key[:16])

    # Checking if the hash lines already exist
    data_user_per_table = session_user_db.query(table_user_db).first()
    hash_already_generated = False
    
    #If the hashed lines already exist then delete it to generate the hash again
    if data_user_per_table: 
        hash_already_generated = True

    searchable_encryption(engine_client_db, engine_user_db, src_table, client_columns_list, table_client_db, table_user_db, master_key, hash_already_generated)