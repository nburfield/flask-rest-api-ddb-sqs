"""
Helper to Authenticate Flask Message API calls
"""

import functools
import jwt

from flask import (
    g, current_app, request, abort
)


def load_auth():
    """
    load_auth Function called before endpoint entry. Sets the Flask g variable with claimset and
    JWT in the Auth header.

    :return None
    """

    # init the flask global values
    g.claimset = {}
    g.jwt = request.headers.get('Authorization', None)

    # if the jwt is not null remove the Bearer
    if g.jwt is not None:
        g.jwt = g.jwt.replace("Bearer", "").strip()

    # get the claimset of the JWT. Do not validate JWT at this time.
    try:
        g.claimset = jwt.decode(g.jwt,
                                key=current_app.config['JWT_SECRET_KEY'],
                                algorithms=['HS256'],
                                options={"verify_signature": True})
    except (jwt.exceptions.InvalidTokenError,
            jwt.exceptions.DecodeError,
            jwt.exceptions.InvalidSignatureError,
            jwt.exceptions.ExpiredSignatureError):
        # JWT errors. Clear the data from Flask g values
        g.claimset = {}
        g.jwt = None
    except Exception as ex:
        g.claimset = {}
        g.jwt = None
        current_app.logger.error(f"JWT Decode Error: {str(ex)}")


def set_auth(response):
    """
    set_auth Function called on endpoint leave. This sets the Auth header with the new JWT.

    :return The response object for flask calls
    """

    # Add CORS to this Flask API
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = 'Content-Type,Authorization'
    response.headers["Access-Control-Allow-Methods"] = 'GET,PUT,PATCH,POST,DELETE,OPTIONS'
    response.headers["Access-Control-Allow-Credentials"] = 'true'

    return response


def require_auth(func):
    """
    require_auth decorator to check if the user is authenticated
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if g.jwt is not None and len(g.claimset) != 0:
            return func(*args, **kwargs)
        abort(401, description="Unauthorized")

    return wrapper
