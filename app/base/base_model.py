"""Base model class providing common database operations and validation."""

import datetime
from app.repositories.repository_factory import RepositoryFactory
from app.base.schema_loader import SchemaLoader


def dt_now():
    """Get current UTC timestamp."""
    return datetime.datetime.utcnow().timestamp()


class BaseModel:
    """Base model class providing common database operations and validation.

    This class provides a standardized interface for database operations across
    different object types. It handles schema validation, data filtering, and
    common CRUD operations.
    """
    object_type = None  # Should be set in subclasses
    database_name = None  # Should be set in subclasses

    def __init__(self):
        if not self.object_type:
            raise ValueError("Subclasses must define object_type")
        if not self.database_name:
            raise ValueError("Subclasses must define database_name")

        # Load schema
        self.schema = SchemaLoader().load_schema(self.object_type)
        self.schema_by_name = {field["name"]: field for field in self.schema}

        # Get the right repository for this object_type
        self.repo = RepositoryFactory.get(
            object_type=self.database_name,
            key_field=self.get_key_field()
        )

    def get_key_field(self):
        """Optionally override per object type if needed (e.g., 'user_id' instead of 'key')"""
        return "key"

    def filter_response_data(self, data: dict, single_object: bool = False) -> dict:
        """
        Filter out fields that should not be included in API responses
        based on the drop_from_response configuration in the schema

        Args:
            data: The data to filter
            single_object: Whether this is a single object response
        """
        if not data:
            return data

        filtered_data = {}
        for field_name, value in data.items():
            # Check if this field exists in our schema
            if field_name in self.schema_by_name:
                field_config = self.schema_by_name[field_name]
                # Only include if drop_from_response is False or not specified
                if not field_config.get("drop_from_response", False):
                    filtered_data[field_name] = value
            else:
                # If field is not in schema, include it (for backward compatibility)
                filtered_data[field_name] = value

        return filtered_data

    def validate_data(self, data: dict, mode: str = "post"):
        """Validate and clean data based on schema for POST or PATCH"""
        if not data:
            raise ValueError("Data cannot be empty")

        result = {}
        for field in self.schema:
            name = field["name"]
            expected_type = field["type"]
            is_nullable = field["null"]
            allow_post = field["post_value"]
            allow_patch = field["patch_value"]
            default = field.get("default")

            # If the field cannot be set with API calls it is set by the system.
            # Only allow pass-through when explicitly running in system mode.
            if mode == "system" and name in data:
                result[name] = data[name]
                continue

            # Skip fields that shouldn't be set in this mode
            if mode == "post" and not allow_post:
                result[name] = default() if callable(default) else default
                continue
            if mode == "patch" and not allow_patch:
                # For patch mode, if the field is already in the data (from existing item),
                # preserve it. Otherwise, skip it.
                if name in data:
                    result[name] = data[name]
                continue

            if name in data:
                value = data[name]
                if value is None:
                    if not is_nullable:
                        raise ValueError(f"{name} cannot be null")
                elif not isinstance(value, expected_type):
                    raise TypeError(f"{name} must be of type {expected_type.__name__}")
                result[name] = value
            else:
                # For PATCH and SYSTEM operations, only include fields explicitly provided
                if mode in ("patch", "system"):
                    continue
                # For POST operations, use defaults for missing required fields
                if not is_nullable and default is None:
                    raise ValueError(f"{name} is required")
                result[name] = default() if callable(default) else default

        return result

    def create(self, data: dict, server_side_overrides: dict = None):
        """Create a new item with validation

        Args:
            data: The request-provided fields to create the item with
            server_side_overrides: Fields set by the server regardless of schema post/patch flags
        """
        try:
            item = self.validate_data(data, mode="post")
            if server_side_overrides:
                # Server-side overrides always win
                item.update(server_side_overrides)
            item["object_type"] = self.object_type
            item["version"] = 0  # Initialize version to 0 for new items

            # Check for unique constraints before creating
            self._check_unique_constraints(item)

            created_item = self.repo.create(item)
            return self.filter_response_data(created_item, single_object=True)
        except Exception as e:
            # Re-raise validation errors as-is
            if isinstance(e, (ValueError, TypeError)):
                raise
            # Wrap repository errors
            raise Exception(f"Failed to create item: {str(e)}") from e

    def update(self, key: str, data: dict, mode: str = "patch", server_side_overrides: dict = None):
        """Update an existing item with validation

        Args:
            key: The key of the item to update
            data: The data to update
            mode: One of {"patch", "system"}. "patch" enforces schema patch rules;
                  "system" allows system-only fields to be set.
        """
        if not key:
            raise ValueError("Key is required for update")

        try:
            # Check if item exists
            existing_item = self.repo.get(key)
            if existing_item is None:
                raise ValueError(f"Item with key '{key}' not found")

            # Separate append-only fields from regular fields
            append_only_operations = {}
            regular_fields = {}

            for field_name, value in data.items():
                if field_name == "version":
                    continue  # Handle version separately

                # Check if this is an append-only field
                if field_name in self.schema_by_name:
                    field_config = self.schema_by_name[field_name]
                    if field_config.get("append_only", False) and field_config["type"] == list:
                        # This is an append-only list field
                        if isinstance(value, list):
                            append_only_operations[field_name] = {
                                'values': value,
                                'use_addToSet': field_config.get("set_append", False)
                            }
                        else:
                            raise ValueError(f"Append-only field '{field_name}' must be a list")
                    else:
                        # Regular field
                        regular_fields[field_name] = value
                else:
                    # Field not in schema, treat as regular field
                    regular_fields[field_name] = value

            # Handle append-only fields first (no version validation needed)
            if append_only_operations:
                updated_item = self.repo.update_append_only_fields(key, append_only_operations)
                if updated_item is None:
                    raise ValueError(f"Failed to update append-only fields for item with key '{key}'")
            else:
                updated_item = existing_item

            # Handle regular fields with version validation
            if regular_fields:
                # Check if version is provided in the update data
                if "version" not in data:
                    raise ValueError("Version field is required for updates")

                expected_version = data["version"]

                # For patch mode, merge the provided data with existing data
                if mode == "patch":
                    # Start with existing data
                    merged_data = updated_item.copy()
                    # Remove system fields that shouldn't be modified
                    for field_name in ["created_dt", "created_user", "_id"]:
                        merged_data.pop(field_name, None)
                    # Update with provided regular fields
                    merged_data.update(regular_fields)
                    # Validate the merged data
                    update_data = self.validate_data(merged_data, mode=mode)
                else:
                    # For system mode, use the provided data directly
                    update_data = self.validate_data(regular_fields, mode=mode)

                # Increment version
                update_data["version"] = expected_version + 1

                # Apply server-side overrides
                if server_side_overrides:
                    # Server-side overrides always win
                    update_data.update(server_side_overrides)

                # Check for unique constraints before updating
                self._check_unique_constraints(update_data, exclude_key=key)

                # Perform atomic update with version checking
                final_item = self.repo.update_by_version(key, update_data, expected_version)

                if final_item is None:
                    # Version mismatch
                    current_version = updated_item.get("version", 0)
                    raise ValueError(f"Version mismatch. Expected version {current_version}, but received {expected_version}")
            else:
                # No regular fields to update, just return the item after append-only updates
                final_item = updated_item

            return self.filter_response_data(final_item, single_object=True)
        except Exception as e:
            # Re-raise validation errors as-is
            if isinstance(e, (ValueError, TypeError)):
                raise
            # Wrap repository errors
            raise Exception(f"Failed to update item with key '{key}': {str(e)}") from e

    def delete(self, key: str):
        """Delete an item"""
        if not key:
            raise ValueError("Key is required for deletion")

        try:
            return self.repo.delete(key)
        except Exception as e:
            raise Exception(f"Failed to delete item with key '{key}': {str(e)}") from e

    def get(self, key: str):
        """Get a single item by key"""
        if not key:
            raise ValueError("Key is required for retrieval")

        try:
            item = self.repo.get(key)
            return self.filter_response_data(item, single_object=True) if item else None
        except Exception as e:
            raise Exception(f"Failed to retrieve item with key '{key}': {str(e)}") from e

    def get_by_field(self, field_name: str, field_value: str):
        """Get a single item by a specific field value"""
        if not field_name or field_value is None:
            raise ValueError("Field name and value are required for retrieval")

        try:
            item = self.repo.get_by_field(field_name, field_value)
            return self.filter_response_data(item, single_object=True) if item else None
        except Exception as e:
            raise Exception(f"Failed to retrieve item by field '{field_name}': {str(e)}") from e

    def list_all(self, filters: dict = None):
        """
        List all items with optional filtering

        Args:
            filters: Parsed filters from QueryParser or legacy filters dict
        """
        try:
            items = self.repo.list_all(self.object_type, filters or {})
            # Filter each item in the list
            return [self.filter_response_data(item, single_object=False) for item in items]
        except Exception as e:
            raise Exception(f"Failed to list items: {str(e)}") from e

    def list_all_paginated(self, filters: dict = None, start: int = 0, limit: int = 50):
        """
        List items with pagination and return total count

        Args:
            filters: Parsed filters from QueryParser
            start: Starting index for pagination
            limit: Maximum number of items to return

        Returns:
            Tuple of (results_list, total_count)
        """
        try:
            # Validate pagination parameters
            if start < 0:
                raise ValueError("Start parameter must be non-negative")
            if limit <= 0:
                raise ValueError("Limit parameter must be positive")
            if limit > 1000:  # Reasonable upper limit
                raise ValueError("Limit parameter cannot exceed 1000")

            results, total_count = self.repo.list_all_paginated(self.object_type, filters or {}, start, limit)
            # Filter each item in the results list
            filtered_results = [self.filter_response_data(item, single_object=False) for item in results]
            return filtered_results, total_count
        except Exception as e:
            # Re-raise validation errors as-is
            if isinstance(e, ValueError):
                raise
            # Wrap repository errors
            raise Exception(f"Failed to list items with pagination: {str(e)}") from e


    def _check_unique_constraints(self, data: dict, exclude_key: str = None):
        """
        Check if any unique field values in the data already exist in the database

        Args:
            data: The data to check for unique constraints
            exclude_key: Optional key to exclude from the check (for updates)

        Raises:
            ValueError: If any unique field values already exist
        """
        # Find fields marked as unique in the schema
        unique_fields = {}
        for field in self.schema:
            if field.get("unique", False) and field["name"] in data:
                unique_fields[field["name"]] = data[field["name"]]

        if not unique_fields:
            return

        # Check for existing values
        conflicting_fields = self.repo.check_unique_values(
            self.object_type,
            unique_fields,
            exclude_key
        )

        if conflicting_fields:
            field_names = ", ".join(conflicting_fields)
            raise ValueError(f"Values for the following fields already exist: {field_names}")
