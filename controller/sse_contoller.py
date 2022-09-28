from controller import app

@app.route('/generate_hash', methods=['GET'])
def generate_hash():
    import service.sse_service as sse_service
    from model import model_user_db

    sse_service.generate_hash('ficticio_database', 'UserDB')

    return {'message': "Hash generated successfully."}, 201