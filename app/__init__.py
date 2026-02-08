"""
Flask Application init
"""

import logging
import os

from flask import Flask

import app.helpers.error as aferror
from app.repositories.repository_factory import RepositoryFactory
from app.services.s3_factory import S3Factory
from app.services.sqs_factory import SQSFactory
from app.api_v1 import health
from app.api_v2 import objects


def create_app():
    """
    create_app Will setup the Flask applicaiton, all blueprints, database connections
    """

    # create and configure the flask_application
    flask_application = Flask(__name__, instance_relative_config=True)

    # load the app config values from the config python file
    app_settings = os.getenv('APP_SETTINGS')
    flask_application.config.from_object(app_settings)

    # Configure logging
    if flask_application.debug:
        flask_application.logger.setLevel(logging.DEBUG)
    else:
        flask_application.logger.setLevel(logging.INFO)

    # Setup repository factory
    RepositoryFactory.configure(
        "dynamo",
        dynamo_client=RepositoryFactory.get_dynamodb_client(flask_application),
        flask_app=flask_application
    )

    # Setup S3 factory
    S3Factory.configure(flask_application)

    # Setup SQS factory
    SQSFactory.configure(flask_application)

    # load api endpoints
    flask_application.register_blueprint(health.bp)
    flask_application.register_blueprint(objects.bp)

    # Setup the Error handlers
    for code in [400, 401, 403, 404, 409, 500]:
        flask_application.register_error_handler(code, aferror.handle_error)

    return flask_application


app = create_app()
