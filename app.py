import os

import service.create_models as create_models
from controller import app, db


@app.route('/')
def index():
    return {'message': "Agent is running."}, 200

@app.route('/create_model_client', methods=['GET'])
def create_model_client():
    create_models.create_model_client(src_db="mysql://root:Dd16012018@localhost:3306/ficticio_database")
    create_models.create_model_user(src_db_client="mysql://root:Dd16012018@localhost:3306/ficticio_database")
    from model import model_user_db
    return {'message': "Client and User Models created successfully."}, 201

@app.route('/initialize_database', methods=['GET'])
def initialize_database():
    os.system("flask db init")
    os.system('flask db migrate -m "Initial migration"')
    os.system('flask db upgrade')
    db.create_all()
    return {'message': "Database initialized successfully."}, 201

@app.route('/generate_hash', methods=['GET'])
def generate_hash():
    import service.SSE as SSE
    SSE.generate_hash('ficticio_database', 'UserDB')
    return {'message': "Hash generated successfully."}, 201

if __name__ == "__main__":
    app.run(debug=True)