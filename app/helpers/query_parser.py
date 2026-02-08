"""Query Parameter Parser for API filtering."""

from datetime import datetime
from typing import Dict, List, Any, Union, Tuple
from decimal import Decimal


class QueryParser:
    """
    Parser for query parameters that supports:
    - Simple equality: ?status=active
    - Multiple values: ?status=active&status=pending
    - Comparison operators: ?created_dt__gt=2023-01-01
    - Date ranges: ?created_dt__gte=2023-01-01&created_dt__lte=2023-12-31
    """

    # Supported comparison operators
    OPERATORS = {
        'eq': '=',      # equals
        'ne': '!=',     # not equals
        'gt': '>',      # greater than
        'gte': '>=',    # greater than or equal
        'lt': '<',      # less than
        'lte': '<=',    # less than or equal
        'in': 'in',     # in list
        'nin': 'nin',   # not in list
        'contains': 'contains',  # contains substring
        'regex': 'regex'  # regex match
    }

    @classmethod
    def parse_query_params(cls, request_args) -> Tuple[Dict[str, Any], str]:
        """
        Parse query parameters into a filter dictionary and filter string

        Args:
            request_args: Flask request.args object

        Returns:
            Tuple of (filters_dict, filter_string)
        """
        filters = {}
        filter_parts = []

        for key, values in request_args.lists():
            # Skip pagination parameters
            if key in ['start', 'limit', 'page', 'per_page']:
                continue

            parsed_key, operator = cls._parse_key(key)
            parsed_values = cls._parse_values(values, operator)

            if parsed_values:
                filters[parsed_key] = {
                    'operator': operator,
                    'values': parsed_values
                }

                # Build filter string for response
                if operator == 'eq':
                    if len(parsed_values) == 1:
                        filter_parts.append(f"{parsed_key}={parsed_values[0]}")
                    else:
                        for value in parsed_values:
                            filter_parts.append(f"{parsed_key}={value}")
                else:
                    for value in parsed_values:
                        filter_parts.append(f"{parsed_key}__{operator}={value}")

        filter_string = "&".join(filter_parts) if filter_parts else ""
        return filters, filter_string

    @classmethod
    def _parse_key(cls, key: str) -> Tuple[str, str]:
        """
        Parse a query parameter key to extract field name and operator

        Args:
            key: Query parameter key (e.g., 'status', 'created_dt__gt')

        Returns:
            Tuple of (field_name, operator)
        """
        if '__' in key:
            field_name, operator = key.rsplit('__', 1)
            if operator in cls.OPERATORS:
                return field_name, operator
            # Invalid operator, treat as simple equality
            return key, 'eq'
        # No operator specified, use equality
        return key, 'eq'

    @classmethod
    def _parse_values(cls, values: List[str], operator: str) -> List[Any]:
        """
        Parse query parameter values based on the operator

        Args:
            values: List of string values from query parameters
            operator: The comparison operator

        Returns:
            List of parsed values
        """
        parsed_values = []

        for value in values:
            if operator in ['gt', 'gte', 'lt', 'lte']:
                # Try to parse as datetime first, then as number
                parsed_value = cls._parse_datetime_or_number(value)
            elif operator in ['in', 'nin']:
                # Split comma-separated values
                parsed_value = [v.strip() for v in value.split(',')]
            else:
                # String values
                parsed_value = value

            parsed_values.append(parsed_value)

        return parsed_values

    @classmethod
    def _parse_datetime_or_number(cls, value: str) -> Union[datetime, int, float, str]:
        """
        Try to parse a value as datetime, then number, then return as string

        Args:
            value: String value to parse

        Returns:
            Parsed value (datetime, int, float, or str)
        """
        # Try common datetime formats
        datetime_formats = [
            '%Y-%m-%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f'
        ]

        for fmt in datetime_formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        # Try to parse as number
        try:
            if '.' in value:
                return float(value)
            # Return as string if all else fails
            return int(value)
        except ValueError:
            pass

        # Return as string if all else fails
        return value


def build_mongo_filter(parsed_filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert parsed filters to MongoDB query format

    Args:
        parsed_filters: Parsed filters from QueryParser

    Returns:
        MongoDB query dictionary
    """
    mongo_filters = {}

    for field, filter_info in parsed_filters.items():
        operator = filter_info['operator']
        values = filter_info['values']

        if operator == 'eq':
            if len(values) == 1:
                mongo_filters[field] = values[0]
            else:
                mongo_filters[field] = {'$in': values}
        elif operator == 'ne':
            mongo_filters[field] = {'$ne': values[0]}
        elif operator == 'gt':
            mongo_filters[field] = {'$gt': values[0]}
        elif operator == 'gte':
            mongo_filters[field] = {'$gte': values[0]}
        elif operator == 'lt':
            mongo_filters[field] = {'$lt': values[0]}
        elif operator == 'lte':
            mongo_filters[field] = {'$lte': values[0]}
        elif operator == 'in':
            mongo_filters[field] = {'$in': values[0]}
        elif operator == 'nin':
            mongo_filters[field] = {'$nin': values[0]}
        elif operator == 'contains':
            mongo_filters[field] = {'$regex': values[0], '$options': 'i'}
        elif operator == 'regex':
            mongo_filters[field] = {'$regex': values[0]}

    return mongo_filters


def build_dynamo_filter(parsed_filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert parsed filters to DynamoDB query format

    Args:
        parsed_filters: Parsed filters from QueryParser

    Returns:
        Dictionary with DynamoDB filter expression and attribute values
    """
    def convert_floats_to_decimals(obj):
        """Convert float values to Decimal for DynamoDB compatibility"""
        if isinstance(obj, dict):
            return {key: convert_floats_to_decimals(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [convert_floats_to_decimals(item) for item in obj]
        if isinstance(obj, float):
            return Decimal(str(obj))
        # Return as is if not float
        return obj

    filter_expressions = []
    expression_attribute_values = {}
    expression_attribute_names = {}

    for field, filter_info in parsed_filters.items():
        operator = filter_info['operator']
        values = filter_info['values']

        # Create attribute name placeholder
        attr_name = f"#{field.replace('.', '_')}"
        expression_attribute_names[attr_name] = field

        if operator == 'eq':
            if len(values) == 1:
                attr_val = f":{field.replace('.', '_')}"
                expression_attribute_values[attr_val] = values[0]
                filter_expressions.append(f"{attr_name} = {attr_val}")
            else:
                # Multiple values - use IN operator
                attr_vals = []
                for i, value in enumerate(values):
                    attr_val = f":{field.replace('.', '_')}_{i}"
                    expression_attribute_values[attr_val] = value
                    attr_vals.append(attr_val)
                filter_expressions.append(f"{attr_name} IN ({', '.join(attr_vals)})")
        elif operator == 'ne':
            attr_val = f":{field.replace('.', '_')}"
            expression_attribute_values[attr_val] = values[0]
            filter_expressions.append(f"{attr_name} <> {attr_val}")
        elif operator == 'gt':
            attr_val = f":{field.replace('.', '_')}"
            expression_attribute_values[attr_val] = values[0]
            filter_expressions.append(f"{attr_name} > {attr_val}")
        elif operator == 'gte':
            attr_val = f":{field.replace('.', '_')}"
            expression_attribute_values[attr_val] = values[0]
            filter_expressions.append(f"{attr_name} >= {attr_val}")
        elif operator == 'lt':
            attr_val = f":{field.replace('.', '_')}"
            expression_attribute_values[attr_val] = values[0]
            filter_expressions.append(f"{attr_name} < {attr_val}")
        elif operator == 'lte':
            attr_val = f":{field.replace('.', '_')}"
            expression_attribute_values[attr_val] = values[0]
            filter_expressions.append(f"{attr_name} <= {attr_val}")
        elif operator == 'contains':
            attr_val = f":{field.replace('.', '_')}"
            expression_attribute_values[attr_val] = values[0]
            filter_expressions.append(f"contains({attr_name}, {attr_val})")

    if not filter_expressions:
        return {}

    # Convert float values to Decimal for DynamoDB compatibility
    expression_attribute_values = convert_floats_to_decimals(expression_attribute_values)

    return {
        'FilterExpression': ' AND '.join(filter_expressions),
        'ExpressionAttributeValues': expression_attribute_values,
        'ExpressionAttributeNames': expression_attribute_names
    }
