import os

from werkzeug.exceptions import HTTPException

from app import api
from app.main import create_app

env_name = os.environ.get("ENV_NAME", "dev")

app = create_app(env_name)
api.init_app(app)


# Global Exception Error
@app.errorhandler(Exception)
def handle_exception(error):
    print(error)
    if isinstance(error, HTTPException):
        return {
            "error": {"message": str(error)},
        }, error.code
    return {
        "error": {
            "message": str(error),
        }
    }, 200


app.app_context().push()


if __name__ == "__main__":
    app.run(host=app.config["HOST"], port=app.config["PORT"])
