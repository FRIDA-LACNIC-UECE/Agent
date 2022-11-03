from flask_migrate import Migrate

from controller import (
    app, db, user_controller, database_controller, sse_contoller,
    verification_controller
)


Migrate(app, db)

@app.route('/')
def index():
    return {'message': "Agent is running."}, 200

if __name__ == "__main__":
    app.run(debug=True, port=3000)