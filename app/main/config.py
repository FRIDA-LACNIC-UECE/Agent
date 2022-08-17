import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    DEBUG = False
    # Remove additional message on 404 responses
    RESTX_ERROR_404_HELP = False

    # Swagger
    RESTX_MASK_SWAGGER = False


class DevelopmentConfig(Config):
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    ENV = "development"
    HOST = "localhost"
    PORT = 3000

    # Informations Api
    API_URL = "http://127.0.0.1:5000"

    # Informations client
    USER_EMAIL = "123"
    USER_PASSWORD = "456"
    USER_ID = 2

    # Informations client database
    ID_DATABASE_CLIENT = 3
    DATABASE_CLIENT_URL = "456"

    # Informations agent database
    DATABASE_AGENT_URL = "698"

    # uncomment the line below to see SQLALCHEMY queries
    # SQLALCHEMY_ECHO = True


class StagingConfig(Config):
    DEBUG = True
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    ENV = "staging"
    PORT = 3000
    HOST = "0.0.0.0"


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(
        basedir, "flask_boilerplate_test.db"
    )
    PRESERVE_CONTEXT_ON_EXCEPTION = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    ENV = "testing"
    PORT = 3000


class ProductionConfig(Config):
    DEBUG = False
    ENV = "production"
    PORT = 3000


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig,
    staging=StagingConfig,
)

_env_name = os.environ.get("ENV_NAME")
_env_name = _env_name if _env_name is not None else "dev"
app_config = config_by_name[_env_name]
