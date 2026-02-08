"""DynamoDB repository implementation for database operations."""

from decimal import Decimal

from app.base.base_repository import BaseRepository
from app.base.schema_loader import SchemaLoader
from app.helpers.query_parser import build_dynamo_filter


def convert_floats_to_decimals(obj):
    """
    Recursively convert float values to Decimal types for DynamoDB compatibility

    Args:
        obj: The object to convert (dict, list, or primitive type)

    Returns:
        The converted object with floats replaced by Decimals
    """
    if isinstance(obj, dict):
        return {key: convert_floats_to_decimals(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [convert_floats_to_decimals(item) for item in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    # Return as is if not float
    return obj


def convert_decimals_to_floats(obj, schema_by_name=None, schema_loader=None):
    """
    Recursively convert Decimal values to appropriate types for JSON serialization.
    Preserves integer types when schema indicates int and Decimal is a whole number.

    Args:
        obj: The object to convert (dict, list, or primitive type)
        schema_by_name: Optional dict mapping field names to schema field definitions
                       with a "type" key indicating the expected type
        schema_loader: Optional SchemaLoader instance for loading schemas on demand

    Returns:
        The converted object with Decimals replaced by floats or ints based on schema
    """
    if isinstance(obj, dict):
        # Try to get schema if we have object_type and schema_loader but no schema_by_name
        if schema_by_name is None and schema_loader is not None and "object_type" in obj:
            try:
                schema = schema_loader.load_schema(obj["object_type"])
                schema_by_name = {field["name"]: field for field in schema}
            except (FileNotFoundError, KeyError):
                # Schema not found, proceed without schema info
                pass

        result = {}
        for key, value in obj.items():
            # If we have schema info and this key is in the schema
            if schema_by_name and key in schema_by_name:
                field_type = schema_by_name[key].get("type")
                # Recursively convert, passing schema for nested structures
                converted_value = convert_decimals_to_floats(value, schema_by_name, schema_loader)
                # If this field should be an int, ensure it's an int
                if field_type == int:
                    if isinstance(converted_value, Decimal):
                        result[key] = int(converted_value) if converted_value % 1 == 0 else float(converted_value)
                    elif isinstance(converted_value, float) and converted_value.is_integer():
                        result[key] = int(converted_value)
                    else:
                        result[key] = converted_value
                # If this field should be a float, ensure it's a float
                elif field_type == float:
                    if isinstance(converted_value, Decimal):
                        result[key] = float(converted_value)
                    else:
                        result[key] = converted_value
                else:
                    result[key] = converted_value
            else:
                # No schema info, convert Decimal to float (preserve type otherwise)
                converted_value = convert_decimals_to_floats(value, schema_by_name, schema_loader)
                if isinstance(converted_value, Decimal):
                    result[key] = float(converted_value)
                else:
                    result[key] = converted_value
        return result
    if isinstance(obj, list):
        return [convert_decimals_to_floats(item, schema_by_name, schema_loader) for item in obj]
    if isinstance(obj, Decimal):
        # Convert Decimal to float by default (schema will handle int conversion in dict context)
        return float(obj)
    # Return as is if not Decimal
    return obj


class DynamoRepository(BaseRepository):
    """DynamoDB repository implementation providing CRUD operations.

    This class implements the BaseRepository interface for DynamoDB,
    handling data conversion, filtering, and pagination.
    """

    def __init__(self, table_name: str, key_field: str = "key", dynamo_client=None):
        super().__init__(key_field)
        if dynamo_client is None:
            raise ValueError("dynamo_client must be provided")
        self.table = dynamo_client.Table(table_name)
        self.schema_loader = SchemaLoader()

    def create(self, item: dict):
        """Create a new item in DynamoDB"""
        try:
            # Convert float values to Decimal for DynamoDB compatibility
            converted_item = convert_floats_to_decimals(item)
            self.table.put_item(Item=converted_item)
            return item  # Return original item, not converted one
        except self.table.meta.client.exceptions.ConditionalCheckFailedException as exc:
            raise Exception("Item already exists with the same key") from exc
        except self.table.meta.client.exceptions.ResourceNotFoundException as exc:
            raise Exception("Table does not exist") from exc
        except Exception as e:
            raise Exception(f"Failed to create item in DynamoDB: {str(e)}") from e

    def update(self, key: str, data: dict):
        """Update an existing item in DynamoDB"""
        if not data:
            # Return existing item if no data to update
            return self.get(key)

        data.pop("key", None)

        try:
            update_expr = []
            expr_attr_vals = {}
            expr_attr_names = {}

            for k, v in data.items():
                # Use expression attribute names to handle reserved keywords
                attr_name = f"#attr_{k}"
                attr_val = f":val_{k}"
                update_expr.append(f"{attr_name} = {attr_val}")
                expr_attr_vals[attr_val] = v
                expr_attr_names[attr_name] = k

            if not update_expr:
                return self.get(key)

            # Convert float values in expression attribute values to Decimal
            expr_attr_vals = convert_floats_to_decimals(expr_attr_vals)

            update_expression = "SET " + ", ".join(update_expr)
            update_kwargs = {
                'Key': {self.key_field: key},
                'UpdateExpression': update_expression,
                'ExpressionAttributeValues': expr_attr_vals,
                'ExpressionAttributeNames': expr_attr_names
            }

            self.table.update_item(**update_kwargs)
            return self.get(key)
        except self.table.meta.client.exceptions.ResourceNotFoundException:
            # Item doesn't exist
            return None
        except Exception as e:
            raise Exception(f"Failed to update item with key '{key}' in DynamoDB: {str(e)}") from e

    def update_by_version(self, key: str, data: dict, expected_version: int):
        """Update an existing item in DynamoDB with version checking"""
        if not data:
            # Return existing item if no data to update
            return self.get(key)

        data.pop("key", None)

        try:
            update_expr = []
            expr_attr_vals = {}
            expr_attr_names = {}

            for k, v in data.items():
                # Use expression attribute names to handle reserved keywords
                attr_name = f"#attr_{k}"
                attr_val = f":val_{k}"
                update_expr.append(f"{attr_name} = {attr_val}")
                expr_attr_vals[attr_val] = v
                expr_attr_names[attr_name] = k

            if not update_expr:
                return self.get(key)

            # Convert float values in expression attribute values to Decimal
            expr_attr_vals = convert_floats_to_decimals(expr_attr_vals)

            update_expression = "SET " + ", ".join(update_expr)

            # Add version condition
            condition_expression = "#version = :expected_version"
            expr_attr_names["#version"] = "version"
            expr_attr_vals[":expected_version"] = expected_version

            update_kwargs = {
                'Key': {self.key_field: key},
                'UpdateExpression': update_expression,
                'ConditionExpression': condition_expression,
                'ExpressionAttributeValues': expr_attr_vals,
                'ExpressionAttributeNames': expr_attr_names
            }

            self.table.update_item(**update_kwargs)
            return self.get(key)
        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            # Version condition failed
            return None
        except self.table.meta.client.exceptions.ResourceNotFoundException:
            # Item doesn't exist
            return None
        except Exception as e:
            raise Exception(f"Failed to update item with key '{key}' in DynamoDB: {str(e)}") from e

    def delete(self, key: str):
        """Delete an item from DynamoDB"""
        try:
            self.table.delete_item(Key={self.key_field: key})
            return True
        except self.table.meta.client.exceptions.ResourceNotFoundException:
            # Item doesn't exist, but deletion is still considered successful
            return True
        except Exception as e:
            raise Exception(f"Failed to delete item with key '{key}' from DynamoDB: {str(e)}") from e

    def get(self, key: str):
        """Get a single item from DynamoDB"""
        try:
            response = self.table.get_item(Key={self.key_field: key})
            item = response.get("Item")
            # Convert Decimal types back to appropriate types for JSON serialization
            return convert_decimals_to_floats(item, schema_loader=self.schema_loader) if item else None
        except self.table.meta.client.exceptions.ResourceNotFoundException as exc:
            # Table doesn't exist
            raise Exception("Table does not exist") from exc
        except Exception as e:
            raise Exception(f"Failed to retrieve item with key '{key}' from DynamoDB: {str(e)}") from e

    def get_by_field(self, field_name: str, field_value: str):
        """
        Get a single item by a specific field value

        Args:
            field_name: Name of the field to search by
            field_value: Value to search for

        Returns:
            The item if found, None otherwise
        """
        try:
            # Build filter expression for the field
            filter_expression = "#field_name = :field_value"
            expression_attr_values = {
                ':field_value': field_value
            }
            expression_attr_names = {
                '#field_name': field_name
            }

            # Convert float values to Decimal for DynamoDB compatibility
            expression_attr_values = convert_floats_to_decimals(expression_attr_values)

            # Scan for items with this field value
            scan_kwargs = {
                'FilterExpression': filter_expression,
                'ExpressionAttributeValues': expression_attr_values,
                'ExpressionAttributeNames': expression_attr_names
            }

            response = self.table.scan(**scan_kwargs)
            items = response.get("Items", [])

            # Return the first item found, or None if no items
            # Convert Decimal types back to appropriate types for JSON serialization
            return convert_decimals_to_floats(items[0], schema_loader=self.schema_loader) if items else None

        except self.table.meta.client.exceptions.ResourceNotFoundException as exc:
            raise Exception("Table does not exist") from exc
        except Exception as e:
            raise Exception(f"Failed to retrieve item by field '{field_name}' from DynamoDB: {str(e)}") from e

    def list_all(self, object_type: str, filters: dict = None):
        """List all items with optional filtering"""
        filters = filters or {}

        try:
            # Convert parsed filters to DynamoDB format
            dynamo_filters = build_dynamo_filter(filters)

            # Add object_type filter
            if dynamo_filters:
                # Add object_type to existing filter expression
                if 'FilterExpression' in dynamo_filters:
                    dynamo_filters['FilterExpression'] = f"object_type = :object_type AND {dynamo_filters['FilterExpression']}"
                else:
                    dynamo_filters['FilterExpression'] = "object_type = :object_type"

                # Add object_type to expression attribute values
                if 'ExpressionAttributeValues' not in dynamo_filters:
                    dynamo_filters['ExpressionAttributeValues'] = {}
                dynamo_filters['ExpressionAttributeValues'][':object_type'] = object_type
            else:
                # No other filters, just filter by object_type
                dynamo_filters = {
                    'FilterExpression': 'object_type = :object_type',
                    'ExpressionAttributeValues': {':object_type': object_type}
                }

            # Convert float values to Decimal for DynamoDB compatibility
            if 'ExpressionAttributeValues' in dynamo_filters:
                dynamo_filters['ExpressionAttributeValues'] = convert_floats_to_decimals(dynamo_filters['ExpressionAttributeValues'])

            scan_kwargs = dynamo_filters
            response = self.table.scan(**scan_kwargs)
            items = response.get("Items", [])
            # Convert Decimal types back to appropriate types for JSON serialization
            return [convert_decimals_to_floats(item, schema_loader=self.schema_loader) for item in items]
        except self.table.meta.client.exceptions.ResourceNotFoundException as exc:
            raise Exception("Table does not exist") from exc
        except Exception as e:
            raise Exception(f"Failed to list items from DynamoDB: {str(e)}") from e

    def list_all_paginated(self, object_type: str, filters: dict = None, start: int = 0, limit: int = 50):
        """
        List items with pagination and return total count

        Args:
            object_type: Type of objects to retrieve
            filters: Parsed filters from QueryParser
            start: Starting index for pagination
            limit: Maximum number of items to return

        Returns:
            Tuple of (results_list, total_count)
        """
        filters = filters or {}

        try:
            # Convert parsed filters to DynamoDB format
            dynamo_filters = build_dynamo_filter(filters)

            # Add object_type filter
            if dynamo_filters:
                # Add object_type to existing filter expression
                if 'FilterExpression' in dynamo_filters:
                    dynamo_filters['FilterExpression'] = f"object_type = :object_type AND {dynamo_filters['FilterExpression']}"
                else:
                    dynamo_filters['FilterExpression'] = "object_type = :object_type"

                # Add object_type to expression attribute values
                if 'ExpressionAttributeValues' not in dynamo_filters:
                    dynamo_filters['ExpressionAttributeValues'] = {}
                dynamo_filters['ExpressionAttributeValues'][':object_type'] = object_type
            else:
                # No other filters, just filter by object_type
                dynamo_filters = {
                    'FilterExpression': 'object_type = :object_type',
                    'ExpressionAttributeValues': {':object_type': object_type}
                }

            # Convert float values to Decimal for DynamoDB compatibility
            if 'ExpressionAttributeValues' in dynamo_filters:
                dynamo_filters['ExpressionAttributeValues'] = convert_floats_to_decimals(dynamo_filters['ExpressionAttributeValues'])

            # Scan the table with filters
            response = self.table.scan(**dynamo_filters)
            items = response.get("Items", [])

            # Convert Decimal types back to appropriate types for JSON serialization
            items = [convert_decimals_to_floats(item, schema_loader=self.schema_loader) for item in items]

            # Handle pagination manually since DynamoDB scan doesn't support offset
            total_count = len(items)
            paginated_items = items[start:start + limit]

            return paginated_items, total_count

        except self.table.meta.client.exceptions.ResourceNotFoundException as exc:
            raise Exception("Table does not exist") from exc
        except Exception as e:
            raise Exception(f"Failed to list items with pagination from DynamoDB: {str(e)}") from e

    def check_unique_values(self, object_type: str, unique_fields: dict, exclude_key: str = None):
        """
        Check if any of the provided unique field values already exist in the database

        Args:
            object_type: Type of objects to check
            unique_fields: Dictionary of field_name: value pairs to check for uniqueness
            exclude_key: Optional key to exclude from the check (for updates)

        Returns:
            List of field names that already exist, empty list if all are unique
        """
        if not unique_fields:
            return []

        try:
            conflicting_fields = []

            # Check each unique field individually
            for field_name, field_value in unique_fields.items():
                if field_value is None:
                    continue

                # Build filter expression for this field
                filter_expression = "object_type = :object_type AND #field_name = :field_value"
                expression_attr_values = {
                    ':object_type': object_type,
                    ':field_value': field_value
                }
                expression_attr_names = {
                    '#field_name': field_name
                }

                # Add exclusion for updates
                if exclude_key:
                    filter_expression += " AND #key_field <> :exclude_key"
                    expression_attr_values[':exclude_key'] = exclude_key
                    expression_attr_names['#key_field'] = self.key_field

                # Convert float values to Decimal for DynamoDB compatibility
                expression_attr_values = convert_floats_to_decimals(expression_attr_values)

                # Scan for existing items with this field value
                scan_kwargs = {
                    'FilterExpression': filter_expression,
                    'ExpressionAttributeValues': expression_attr_values,
                    'ExpressionAttributeNames': expression_attr_names
                }

                response = self.table.scan(**scan_kwargs)

                # If any items found, this field value is not unique
                if response.get("Items"):
                    conflicting_fields.append(field_name)

            return conflicting_fields

        except self.table.meta.client.exceptions.ResourceNotFoundException as exc:
            raise Exception("Table does not exist") from exc
        except Exception as e:
            raise Exception(f"Failed to check unique values in DynamoDB: {str(e)}") from e
