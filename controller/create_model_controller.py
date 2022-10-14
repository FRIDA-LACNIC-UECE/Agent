from controller import app, db
from service import create_model_service


@app.route('/create_model_client', methods=['GET'])
def create_model_client():
    create_model_service.create_model_client(
        src_db="mysql://root:Dd16012018@localhost:3306/ficticio_database"
    )
    create_model_service.create_model_user(src_db_client="mysql://root:Dd16012018@localhost:3306/ficticio_database")
    
    return {'message': "Client and User Models created successfully."}, 201