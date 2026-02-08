"""
Generic Object API
"""

import importlib
import json

from flask import (
    Blueprint, request, abort, current_app, g
)
from werkzeug.exceptions import HTTPException
from app.helpers.auth_helper import (
    load_auth, set_auth, require_auth
)


bp = Blueprint('objects', __name__, url_prefix='/api/v2')
bp.before_request(load_auth)
bp.after_request(set_auth)


plurals = {
    'foobars': 'foobar'
}


def load_class(module_base, object_type, class_suffix):
    """Load a class dynamically from a module based on object type and class suffix."""
    try:
        object_type = plurals.get(object_type, object_type)
        module_path = f"app.{module_base}.{object_type}"
        module = importlib.import_module(module_path)
        cls = getattr(module, f"{object_type.capitalize()}{class_suffix}")
        return cls
    except (ImportError, AttributeError) as ex:
        current_app.logger.error(f"Error loading class: {str(ex)}")
        abort(404, description=f"{object_type} not supported")


def validate_json_request():
    """Validate that the request contains valid JSON data"""
    if not request.is_json:
        abort(400, description="Content-Type must be application/json")

    if not request.data:
        abort(400, description="Request body is required")

    try:
        return request.get_json()
    except json.JSONDecodeError:
        abort(400, description="Invalid JSON in request body")


def get_user_from_request():
    """Extract user information from the request context."""
    # Get user from Flask g context (set by auth middleware)
    user = g.get('claimset', {}).get('user_name', 'unknown')
    return user


@bp.route('/<object_type>', methods=['GET', 'POST'])
@bp.route('/<object_type>/<string:item_id>', methods=['GET', 'PATCH', 'DELETE'])
# @require_auth
def handle_crud(object_type, item_id=None):
    """Handle CRUD operations for generic objects.

    Args:
        object_type: The type of object to operate on
        item_id: Optional ID for specific item operations

    Returns:
        Response object with appropriate status code and data
    """
    try:
        helper_class = load_class("helpers", object_type, "Helper")

        helper = helper_class()

        if request.method == 'GET':
            if item_id:
                return helper.get_by_key(item_id)
            # Pass query parameters for filtering
            return helper.get_all(query_params=request.args)
        if request.method == 'POST':
            # Validate JSON request for POST
            data = validate_json_request()
            user = get_user_from_request()
            return helper.create(data, user)
        if request.method == 'PATCH':
            # Validate JSON request for PATCH
            data = validate_json_request()
            user = get_user_from_request()
            return helper.update(item_id, data, user)
        if request.method == 'DELETE':
            return helper.delete(item_id)
        # This should never happen due to route decorators, but handle it
        abort(405, description="Method not allowed")
    except HTTPException as e:
        abort(e.code, description=e.description)
    except Exception as ex:
        current_app.logger.error(f"Internal server error: {str(ex)}")
        abort(500, description="Internal server error")
