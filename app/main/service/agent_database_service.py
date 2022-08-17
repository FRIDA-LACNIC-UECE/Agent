from sqlalchemy import Column, Integer, MetaData, Table, Text, create_engine, inspect
from sqlalchemy.orm import Session, joinedload
from sqlalchemy_utils import create_database, database_exists
from werkzeug.datastructures import ImmutableMultiDict

from app.main.config_client import ConfigClient
from app.main.exceptions import DefaultException, ValidationException


def agent_database_start() -> None:

    # Creating connection with client database
    engine_client_db = create_engine(ConfigClient.CLIENT_DATABASE_URL)

    if not database_exists(ConfigClient.AGENT_DATABASE_URL):
        create_database(ConfigClient.AGENT_DATABASE_URL)

    # Creating connection with agent database
    engine_agent_db = create_engine(ConfigClient.AGENT_DATABASE_URL)

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_agent_db._metadata = MetaData(bind=engine_agent_db)
    engine_agent_db._metadata.reflect(
        engine_agent_db
    )  # get columns from existing table

    for table in list(engine_client_db.table_names()):

        table_agent_db = Table(
            table,
            engine_agent_db._metadata,
            Column("primary_key", Integer),
            Column("line_hash", Text),
        )

        table_agent_db.create()
