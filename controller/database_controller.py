import os

from controller import app, db
from sqlalchemy_utils import create_database, database_exists


@app.route('/initialize_database', methods=['GET'])
def initialize_database():
    from model import model_user_db
    
    url = "mysql://root:Dd16012018@localhost:3306/UserDB"
    
    if not database_exists(url):
        create_database(url)

    os.system("flask db init")
    os.system('flask db migrate -m "Initial migration"')
    os.system('flask db upgrade')

    db.create_all()

    return {'message': "Database initialized successfully."}, 201