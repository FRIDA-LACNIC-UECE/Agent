import time
from datetime import datetime
import threading
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
import models


def checking_changes():

    # Getting Metadata
    table_names = []
    classes_db = {}

    for i in range (1, 6):
        tn = "Tabela" + str(i)
        table_names.append(tn)
        classes_db[f"{tn}"] = eval(f"models.{tn}")

    # Connecting on Local DataBase
    localDB_name = "LocalDB"
    engine_local_db = create_engine(f"sqlite:///{localDB_name}")
    session_local_db = Session(engine_local_db)

    # Connecting on Cloud DataBase
    cloudDB_name = "CloudDB"
    engine_cloud_db = create_engine(f"sqlite:///{cloudDB_name}")
    session_cloud_db = Session(engine_cloud_db)
    
    print("Initialized Check")

    for name in table_names:
        lines_hash_local = list(session_local_db.query(classes_db[f"{name}"].line_hash).all())
        lines_hash_cloud = list(session_cloud_db.query(classes_db[f"{name}"].line_hash).all())
        
        count = 1

        for line_hash_local, line_hash_cloud in zip(lines_hash_local, lines_hash_cloud):
            #print(f"{name} - {count}")
            #print(line_hash_local[0])
            #print(line_hash_cloud[0])
            if line_hash_local[0] != line_hash_cloud[0]:
                return "There are changes"

            count += 1

            print(f"Checking: {name} - {count}")

    print(f"Finished Check - {datetime.now()}")
    return "No changes"

def check_thread():
    # Set time period of task
    seconds_task = 30

    # Running Task
    while True:
        # Checking runtime
        print(checking_changes())
        print()
        time.sleep(seconds_task)

if __name__ == "__main__":
    # Run task on thread
    thread = threading.Thread(target=check_thread)
    thread.start()

    # Set time period of main task
    seconds_task = 10

    while True:
        # Run main task
        print(f"Running Main Task! - {datetime.now()}")
        print()
        time.sleep(seconds_task)
    