"""
Validation utilities for user data
"""
import re
import uuid
from typing import Optional, List
import hashlib
from datetime import datetime
import phonenumbers


def validate_email(email: str) -> str:
    """
    Validate email address format

    Args:
        email: Email address to validate

    Returns:
        The validated email address

    Raises:
        ValueError: If email is invalid
    """
    if not email:
        raise ValueError("Email cannot be empty")

    # Basic email regex pattern
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    if not re.match(email_pattern, email):
        raise ValueError("Invalid email format")

    return email.lower().strip()


def validate_phone(phone: Optional[str]) -> Optional[str]:
    """
    Validate and format phone number using phonenumbers library

    Args:
        phone: Phone number to validate (can be None)

    Returns:
        Formatted phone number or None if input was None

    Raises:
        ValueError: If phone number is invalid
    """
    if phone is None:
        return None

    if not phone.strip():
        return None

    try:
        # Parse the phone number (assume US if no country code)
        parsed_number = phonenumbers.parse(phone, "US")

        # Check if the number is valid
        if not phonenumbers.is_valid_number(parsed_number):
            raise ValueError("Invalid phone number")

        # Format the number in E.164 format (international format)
        formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)

        return formatted_number

    except phonenumbers.NumberParseException as e:
        raise ValueError(f"Invalid phone number format: {str(e)}") from e


def validate_status(status: str, allowed_values: List[str]) -> str:
    """
    Validate user status against allowed values from schema

    Args:
        status: Status to validate
        allowed_values: List of allowed status values from schema

    Returns:
        The validated status

    Raises:
        ValueError: If status is not in allowed list
    """
    if status not in allowed_values:
        raise ValueError(f"Status must be one of: {', '.join(allowed_values)}")

    return status


def generate_foobar_key() -> str:
    """
    Generate a unique key for foobar objects

    Returns:
        A unique key string
    """
    hash_m = hashlib.sha1()
    hash_m.update(f"foobar_{uuid.uuid4().hex[:12]}".encode())
    return hash_m.hexdigest()
