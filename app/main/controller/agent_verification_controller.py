import threading

from flask_restx import Resource

from app.main.service import create_verification_thread
from app.main.util import AgentVerification, DefaultResponsesDTO

agent_verification_ns = AgentVerification.api
api = agent_verification_ns

_default_message_response = DefaultResponsesDTO.message_response


@api.route("/start")
class AgentVerification(Resource):
    @api.doc("Agent verification")
    @api.response(200, "start_agent_verification", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(401, "token_not_found\ntoken_invalid", _default_message_response)
    def post(self) -> tuple[dict[str, str], int]:
        """Start agent verification"""
        thread = threading.Thread(target=create_verification_thread)
        thread.start()
        return {"message": "start_agent_verification"}, 200
