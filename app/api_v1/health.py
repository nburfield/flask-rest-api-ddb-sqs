"""
Sanity check API for the Flask application
"""

from flask import (
    Blueprint, jsonify, current_app, request
)

from app.helpers.api_helper import (
    make_api_message
)
from app.helpers.auth_helper import (
    set_auth
)


bp = Blueprint('health', __name__, url_prefix='/api/v1')
bp.after_request(set_auth)


@bp.route('/health/flask', methods=('GET',))
def get_health_flask():
    """
    get_health_flask API call to verify the health of the Flask Application

    :return A JSON of a data object with a message
    """

    data = make_api_message("success", "Flask is running")
    return jsonify(data)


@bp.route('/health/userdata', methods=('GET',))
def get_health_userdata():
    """
    get_health_userdata API call to verify the userdata of the Flask Application.
    Checkes the userdata connection is valid.

    :return A JSON of a data object with a message
    """

    try:
        headers = "] [".join([f"{key}:{value}" for key, value in request.headers.items() if key not in ["Authorization", "Accept"]])
        current_app.logger.info("[USERDATA] [%s]", headers)

        data = make_api_message("success", headers)
        return jsonify(data)
    except Exception as ex:
        current_app.logger.error(ex)
        data = make_api_message("error", "Error Health Check")
        return jsonify(data), 400
