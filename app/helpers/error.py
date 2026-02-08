"""
Flask Error Endpoints
"""

from flask import (
    jsonify
)

from app.helpers.api_helper import (
    make_api_message
)


def handle_error(error):
    """
    handle_error Handle custom errors

    :return JSON with error message
    """

    # Create the API message
    return_data = make_api_message(error.code, error.description if error.description else "An error occurred")

    return jsonify(return_data), error.code
