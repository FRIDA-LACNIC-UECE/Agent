from flask_restx import Namespace


class AgentDatabase:
    api = Namespace("agent_database", description="Agent database related operations")
