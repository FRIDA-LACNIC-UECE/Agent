from flask import jsonify

from controller import (
    agent_controller,
    app,
    database_controller,
    verification_controller,
)


@app.route("/")
def index():
    return jsonify({"message": "Agent is running."}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=3000)
