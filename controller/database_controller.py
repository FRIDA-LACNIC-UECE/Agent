import os

from flask import jsonify, request
from sqlalchemy_utils import create_database, database_exists

from controller import app, db
from model.databases_model import Database, database_share_schema, databases_share_schema
from model.valid_database_model import ValidDatabase, valid_databases_share_schema


@ app.route('/getDatabases', methods=['GET'])
def getDatabases():
    try:

        result_databases = databases_share_schema.dump(
            Database.query.all()
        )

        result_valid_databases = valid_databases_share_schema.dump(
            ValidDatabase.query.all()
        )

        for database in result_databases:
            for type in result_valid_databases:
                if type['id'] == database['id_db_type']:
                    database['name_db_type'] = type['name']
    except:
        return jsonify({'message': 'database_invalid_data'}), 400

    return jsonify(result_databases), 200


@app.route('/addDatabase', methods=['POS'])
def addDatabase():
    try:
        # Get clinet database information
        id_db_type = request.json.get('id_db_type')
        name = request.json.get('name')
        host = request.json.get('host')
        user = request.json.get('user')
        port = request.json.get('port')
        pwd = request.json.get('password')

        database = Database(id_db_type, name, host, user, port, pwd, '')
        db.session.add(database)
        db.session.flush()
    except:
        db.session.rollback()
        return jsonify({'message': 'database_invalid_data'}), 400

    # Commit updates
    db.session.commit()

    return jsonify({'message': 'database_added'}), 201


@ app.route('/deleteDatabase', methods=['DELETE'])
def deleteDatabase():
    try:
        id_db = request.json.get('id_db')
        database = Database.query.filter_by(id=id_db).first()

        if not database:
            return jsonify({'message': 'database_not_found'}), 404

        db.session.delete(database)
        db.session.commit()

        result = database_share_schema.dump(
            Database.query.filter_by(id=id).first()
        )
    except:
        return jsonify({'message': 'database_not_deleted'}), 500

    if not result:
        return jsonify({'message': 'database_deleted'}), 200
    else:
        return jsonify({'message': 'database_not_deleted'}), 500


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