from flask import request
from flask_restx import Resource

from app.main.service import agent_start
from app.main.util import AgentStartDTO, DefaultResponsesDTO

agent_start_ns = AgentStartDTO.api
api = agent_start_ns

_agent_start = AgentStartDTO.agent_start

_default_message_response = DefaultResponsesDTO.message_response


@api.route("")
class AgentStart(Resource):
    @api.doc("Start Agent")
    @api.expect(_agent_start, validate=True)
    @api.response(200, "agent_started", _default_message_response)
    @api.response(400, "Input payload validation failed", _default_message_response)
    @api.response(404, "client_database_not_found", _default_message_response)
    @api.response(
        500,
        "client_config_not_loaded\nuser_not_logged_in\nclient_databases_not_loaded",
        _default_message_response,
    )
    def post(self) -> tuple[dict[str, str], int]:
        """Start Agent"""
        data = request.json
        agent_start(data=data)
        return {"message": "agent_started"}, 200
