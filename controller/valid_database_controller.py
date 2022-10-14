from flask import jsonify, request

from controller import app, db

from model.valid_database_model import ValidDatabase, valid_databases_share_schema


@ app.route('/getValidDatabases', methods=['GET'])
def getValidDatabases():

    result = valid_databases_share_schema.dump(
        ValidDatabase.query.all()
    )

    return jsonify(result)


@ app.route('/addValidDatabase', methods=['POST'])
def addValidDatabase():
    name = request.json['name']

    valid_database = ValidDatabase(name=name)
    db.session.add(valid_database)
    db.session.commit()

    return jsonify({
        'message': 'Valid database added successfully!'
    })
    