from flask_restx import Resource

from app.main.service import agent_database_start
from app.main.util import AgentDatabase, DefaultResponsesDTO

agent_database_ns = AgentDatabase.api
api = agent_database_ns

_default_message_response = DefaultResponsesDTO.message_response


@api.route("/start")
class AgentDatabaseStart(Resource):
    @api.doc("Agent database start")
    @api.response(200, "agent_database_started", _default_message_response)
    @api.response(500, "agent_database_not_started", _default_message_response)
    def post(self) -> tuple[dict[str, str], int]:
        """Agent database start"""
        agent_database_start()
        return {"message": "agent_database_started"}, 200
