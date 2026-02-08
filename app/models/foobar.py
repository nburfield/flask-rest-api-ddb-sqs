"""Foobar model for handling foobar data and validation."""

from app.base.base_model import BaseModel
from app.helpers.validation import validate_email, validate_phone, validate_status, generate_foobar_key


class FoobarModel(BaseModel):
    """Foobar model providing foobar-specific validation and key generation.

    This model extends BaseModel to provide foobar-specific functionality
    including email validation, phone validation, status validation,
    and automatic key generation.
    """
    object_type = "foobar"
    database_name = "foobars"

    def validate_data(self, data: dict, mode: str = "post"):
        """
        Override validate_data to add custom validation for foobar-specific fields
        """
        # First run the base validation
        validated_data = super().validate_data(data, mode)

        # Apply custom validation for specific fields
        if "email" in validated_data:
            validated_data["email"] = validate_email(validated_data["email"])

        if "phone" in validated_data:
            validated_data["phone"] = validate_phone(validated_data["phone"])

        if "status" in validated_data:
            # Get allowed values from schema
            status_field = next((field for field in self.schema if field["name"] == "status"), None)
            if status_field and "allowed_values" in status_field:
                validated_data["status"] = validate_status(validated_data["status"], status_field["allowed_values"])

        return validated_data

    def create(self, data: dict, server_side_overrides: dict = None):
        """
        Override create to auto-generate the key field before creation
        """
        # Ensure a key exists and is preserved through validation by using server-side overrides
        key_value = data.get("key") or generate_foobar_key()

        overrides = dict(server_side_overrides or {})
        overrides["key"] = key_value

        return super().create(data, server_side_overrides=overrides)

    def get_by_field(self, field_name: str, field_value: str, unfiltered: bool = False):
        """Get a single item by a specific field value"""
        if not field_name or field_value is None:
            raise ValueError("Field name and value are required for retrieval")

        try:
            item = self.repo.get_by_field(field_name, field_value)
            if unfiltered:
                return item
            return self.filter_response_data(item) if item else None
        except Exception as e:
            raise Exception(f"Failed to retrieve item by field '{field_name}': {str(e)}") from e
