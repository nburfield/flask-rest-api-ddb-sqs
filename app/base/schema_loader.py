"""Schema loader for loading and parsing JSON schema files."""

import json
import os
from datetime import datetime


def dt_now():
    """Get current UTC timestamp."""
    return datetime.utcnow().timestamp()


# Map string types from JSON to actual Python types/functions
TYPE_MAP = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "list": list,
    "dict": dict,
    "dt_now": dt_now,
    "null": None
}


class SchemaLoader:
    """Schema loader for loading and parsing JSON schema files.
    
    This class handles loading schema definitions from JSON files and converting
    them into Python objects with proper type mappings.
    """

    def __init__(self, schema_dir=None):
        """Initialize the schema loader.
        
        Args:
            schema_dir: Optional custom directory for schema files
        """
        if schema_dir is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.schema_dir = os.path.join(current_dir, "..", "schemas")
        else:
            self.schema_dir = schema_dir

        self.schema_dir = os.path.abspath(self.schema_dir)

    def load_schema(self, object_type):
        """Load and parse a schema file for the specified object type.
        
        Args:
            object_type: The type of object whose schema to load
            
        Returns:
            List of parsed field definitions
            
        Raises:
            FileNotFoundError: If the schema file doesn't exist
        """
        schema_file = os.path.join(self.schema_dir, f"{object_type}.json")

        if not os.path.exists(schema_file):
            raise FileNotFoundError(f"Schema for '{object_type}' not found: {schema_file}")

        with open(schema_file, encoding='utf-8') as f:
            raw_fields = json.load(f)

        parsed_fields = []
        for field in raw_fields:
            parsed_field = dict(field)  # copy to avoid mutation

            # Convert type string to actual Python type
            parsed_field["type"] = TYPE_MAP.get(parsed_field["type"], str)

            # Convert default string to function/value
            default = parsed_field.get("default")
            if isinstance(default, str) and default in TYPE_MAP:
                parsed_field["default"] = TYPE_MAP[default]
            elif default is None:
                parsed_field["default"] = None

            parsed_fields.append(parsed_field)

        return parsed_fields
