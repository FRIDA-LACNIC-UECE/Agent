from flask_restx import Namespace


class AgentVerification:
    api = Namespace(
        "agent_verification", description="Agent verification related operations"
    )
