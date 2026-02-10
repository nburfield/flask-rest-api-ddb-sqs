"""
Base Helper class for all helpers
"""

from flask import jsonify, abort, current_app
from werkzeug.exceptions import HTTPException

from app.helpers.query_parser import QueryParser
from app.helpers.api_helper import make_list_api_response, get_start_limit


class BaseHelper:
    """Base helper class providing common CRUD operations for all models.

    This class provides a standardized interface for creating, reading, updating,
    and deleting items across different model types. It handles common validation,
    error handling, and response formatting.
    """
    def __init__(self, model_class):
        self.model = model_class()

    def _get_user_roles(self):
        """Extract user roles from JWT claimset"""
        if not hasattr(g, 'claimset') or not g.claimset:
            return []
        return g.claimset.get('roles', [])

    def _filter_fields_by_role(self, data):
        """
        Filter update data based on user role permissions defined in the schema.

        For each field in the update data:
        - If the field has 'update_roles' defined in the schema, check if the user
          has one of those roles. If not, filter out the field.
        - If the field does not have 'update_roles' defined, allow it (no restrictions).
        - Always allow 'version' field for optimistic locking.

        Args:
            data: The update data dictionary

        Returns:
            Filtered data dictionary with only allowed fields
        """
        user_roles = self._get_user_roles()
        filtered_data = {}
        removed_fields = []

        for field_name, value in data.items():
            # Always allow version field for optimistic locking
            if field_name == 'version':
                filtered_data[field_name] = value
                continue

            # Check if field exists in schema
            if field_name not in self.model.schema_by_name:
                # Field not in schema, remove it
                continue

            field_config = self.model.schema_by_name[field_name]
            update_roles = field_config.get('update_roles')

            # If update_roles is not defined, there are no restrictions
            if update_roles is None:
                filtered_data[field_name] = value
                continue

            # Check if user has one of the required roles
            if any(role in user_roles for role in update_roles):
                filtered_data[field_name] = value
            else:
                removed_fields.append(field_name)

        if removed_fields:
            current_app.logger.info(
                f"User with roles {user_roles} attempted to update restricted fields: {removed_fields}. "
                f"These fields were filtered out."
            )

        return filtered_data

    def get_all(self, query_params=None):
        """
        Get all items with optional filtering and pagination

        Args:
            query_params: Flask request.args object for query parameter filtering
        """
        try:
            # Parse filters (excluding pagination params)
            parsed_filters, filter_string = QueryParser.parse_query_params(query_params)

            # Get pagination parameters
            start, limit, pagination_filter = get_start_limit(
                query_params or {},
                start_default=0,
                limit_default=50,
                current_filter=filter_string
            )

            # Combine filter strings
            if pagination_filter and filter_string:
                combined_filter = f"{filter_string}&{pagination_filter}"
            elif pagination_filter:
                combined_filter = pagination_filter
            else:
                combined_filter = filter_string

            # Get paginated results and total count
            results, total_count = self.model.list_all_paginated(parsed_filters, start, limit)

            # Determine if this is the last page
            is_last = (start + limit) >= total_count

            return jsonify(make_list_api_response(
                results,
                start,
                limit,
                is_last,
                combined_filter,
                total_count
            )), 200

        except (ValueError, TypeError) as e:
            # Invalid query parameters or pagination values
            current_app.logger.warning(f"Invalid query parameters: {str(e)}")
            abort(400, description=f"Invalid query parameters: {str(e)}")
        except HTTPException as e:
            abort(e.code, description=e.description)
        except Exception as e:
            # Unexpected error during list operation
            current_app.logger.error(f"Error retrieving items: {str(e)}")
            abort(500, description="Internal server error while retrieving items")

    def get_by_key(self, key):
        """
        Get a single item by key

        Args:
            key: The key of the item to retrieve
        """
        if not key:
            abort(400, description="Key parameter is required")

        try:
            item = self.model.get(key)
            if item is None:
                abort(404, description=f"Item with key '{key}' not found")
            return jsonify(item), 200

        except HTTPException as e:
            abort(e.code, description=e.description)
        except Exception as e:
            current_app.logger.error(f"Error retrieving item with key '{key}': {str(e)}")
            abort(500, description="Internal server error while retrieving item")

    def create(self, data, user):
        """
        Create a new item

        Args:
            data: The data for the new item
            user: The user creating the item
        """
        if not data:
            abort(400, description="Request body is required")

        try:
            # Server-side overrides for system-maintained fields
            overrides = {
                "created_user": user,
                "created_dt": self.model.schema_by_name["created_dt"]["default"](),
                "updated_user": user,
                "updated_dt": self.model.schema_by_name["updated_dt"]["default"](),
            }

            created_item = self.model.create(data, server_side_overrides=overrides)
            return jsonify(created_item), 201

        except ValueError as e:
            # Validation error (missing required fields, etc.)
            current_app.logger.warning(f"Validation error during create: {str(e)}")
            abort(400, description=f"Validation error: {str(e)}")
        except TypeError as e:
            # Type validation error
            current_app.logger.warning(f"Type validation error during create: {str(e)}")
            abort(400, description=f"Type validation error: {str(e)}")
        except HTTPException as e:
            abort(e.code, description=e.description)
        except Exception as e:
            # Unexpected error during creation
            current_app.logger.error(f"Error creating item: {str(e)}")
            abort(500, description="Internal server error while creating item")

    def update(self, key, data, user, *, mode: str = "patch"):
        """
        Update an existing item

        Args:
            key: The key of the item to update
            data: The data to update
            user: The user updating the item
        """
        if not key:
            abort(400, description="Key parameter is required")
        if not data:
            abort(400, description="Request body is required")

        try:
            # Check if item exists first
            existing_item = self.model.get(key)
            if existing_item is None:
                abort(404, description=f"Item with key '{key}' not found")

            # Server-side overrides for system-maintained fields
            overrides = {
                "updated_user": user,
                "updated_dt": self.model.schema_by_name["updated_dt"]["default"](),
            }

            updated_item = self.model.update(key, data, mode=mode, server_side_overrides=overrides)
            if updated_item is None:
                # This shouldn't happen if we checked existence above, but handle it
                abort(404, description=f"Item with key '{key}' not found")

            return jsonify(updated_item), 200

        except ValueError as e:
            error_msg = str(e)
            # Check if this is a version-related error
            if "version" in error_msg.lower():
                if "required" in error_msg.lower():
                    current_app.logger.warning(f"Version field missing in update request: {error_msg}")
                    abort(400, description=f"Version field is required for updates: {error_msg}")
                elif "mismatch" in error_msg.lower():
                    current_app.logger.warning(f"Version conflict during update: {error_msg}")
                    abort(409, description=f"Version conflict: {error_msg}")
                else:
                    current_app.logger.warning(f"Version validation error during update: {error_msg}")
                    abort(400, description=f"Version validation error: {error_msg}")
            else:
                # Other validation errors
                current_app.logger.warning(f"Validation error during update: {error_msg}")
                abort(400, description=f"Validation error: {error_msg}")
        except TypeError as e:
            # Type validation error
            current_app.logger.warning(f"Type validation error during update: {str(e)}")
            abort(400, description=f"Type validation error: {str(e)}")
        except HTTPException as e:
            abort(e.code, description=e.description)
        except Exception as e:
            # Unexpected error during update
            current_app.logger.error(f"Error updating item with key '{key}': {str(e)}")
            abort(500, description="Internal server error while updating item")

    def delete(self, key):
        """
        Delete an item

        Args:
            key: The key of the item to delete
        """
        if not key:
            abort(400, description="Key parameter is required")

        try:
            # Check if item exists first
            existing_item = self.model.get(key)
            if existing_item is None:
                abort(404, description=f"Item with key '{key}' not found")

            result = self.model.delete(key)

            # Return appropriate response based on repository implementation
            if result is True or result is None:
                # Success - return 204 No Content for successful deletion
                return "", 204
            # Some repositories might return the deleted item
            return jsonify(result), 200
        except HTTPException as e:
            abort(e.code, description=e.description)
        except Exception as e:
            current_app.logger.error(f"Error deleting item with key '{key}': {str(e)}")
            abort(500, description="Internal server error while deleting item")
