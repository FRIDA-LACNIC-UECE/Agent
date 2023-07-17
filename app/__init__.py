from flask_restx import Api

from .main.controller import agent_database_ns, agent_start_ns, agent_verification_ns
from .main.exceptions import DefaultException, ValidationException
from .main.util import DefaultResponsesDTO

authorizations = {"apikey": {"type": "apiKey", "in": "header", "name": "Authorization"}}

api = Api(
    authorizations=authorizations,
    title="SysPAD Monitor Agent",
    version="1.0",
    description="Database Changes Monitor",
    security="apikey",
)

api.add_namespace(agent_start_ns, path="/agent_start")
api.add_namespace(agent_database_ns, path="/agent_database")
api.add_namespace(agent_verification_ns, path="/agent_verification")

api.add_namespace(DefaultResponsesDTO.api)

# Exception Handler
api.errorhandler(DefaultException)


@api.errorhandler(ValidationException)
def handle_validation_exception(error):
    """Return a list of errors and a message"""
    return {"errors": error.errors, "message": error.message}, error.code
