from flask_restx import Namespace, fields


class AgentStartDTO:
    api = Namespace("agent_start", description="Agent start related operations")

    agent_start = api.model(
        "agent_start",
        {
            "database_id": fields.Integer(
                required=True,
                description="client database id",
            ),
            "user_email": fields.String(required=True, description="user email"),
            "user_password": fields.String(required=True, description="user password"),
        },
    )
