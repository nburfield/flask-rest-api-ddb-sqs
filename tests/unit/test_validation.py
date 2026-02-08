"""Tests for app.helpers.validation module."""

import pytest
from app.helpers.validation import (
    validate_email,
    validate_phone,
    validate_status,
    generate_foobar_key
)


class TestValidateEmail:

    def test_valid_email(self):
        result = validate_email("user@example.com")
        assert result == "user@example.com"

    def test_valid_email_uppercase_normalized(self):
        result = validate_email("User@Example.COM")
        assert result == "user@example.com"

    def test_email_with_whitespace_rejected(self):
        # The regex validates before stripping, so whitespace causes rejection
        with pytest.raises(ValueError, match="Invalid email format"):
            validate_email("  user@example.com  ")

    def test_valid_email_with_plus_sign(self):
        result = validate_email("user+tag@example.com")
        assert result == "user+tag@example.com"

    def test_valid_email_with_dots(self):
        result = validate_email("first.last@example.com")
        assert result == "first.last@example.com"

    def test_valid_email_with_subdomain(self):
        result = validate_email("user@mail.example.com")
        assert result == "user@mail.example.com"

    def test_invalid_email_no_at(self):
        with pytest.raises(ValueError, match="Invalid email format"):
            validate_email("userexample.com")

    def test_invalid_email_no_domain(self):
        with pytest.raises(ValueError, match="Invalid email format"):
            validate_email("user@")

    def test_invalid_email_no_tld(self):
        with pytest.raises(ValueError, match="Invalid email format"):
            validate_email("user@example")

    def test_invalid_email_double_at(self):
        with pytest.raises(ValueError, match="Invalid email format"):
            validate_email("user@@example.com")

    def test_empty_email(self):
        with pytest.raises(ValueError, match="Email cannot be empty"):
            validate_email("")

    def test_none_email(self):
        with pytest.raises(ValueError, match="Email cannot be empty"):
            validate_email(None)

    def test_email_with_special_chars(self):
        result = validate_email("user.name+tag@example.co.uk")
        assert result == "user.name+tag@example.co.uk"


class TestValidatePhone:

    def test_valid_us_phone_e164(self):
        result = validate_phone("+12025551234")
        assert result == "+12025551234"

    def test_valid_us_phone_without_country_code(self):
        result = validate_phone("2025551234")
        assert result == "+12025551234"

    def test_valid_us_phone_with_parentheses(self):
        result = validate_phone("(202) 555-1234")
        assert result == "+12025551234"

    def test_valid_us_phone_with_dashes(self):
        result = validate_phone("202-555-1234")
        assert result == "+12025551234"

    def test_none_phone(self):
        result = validate_phone(None)
        assert result is None

    def test_empty_phone(self):
        result = validate_phone("")
        assert result is None

    def test_whitespace_phone(self):
        result = validate_phone("   ")
        assert result is None

    def test_invalid_phone_too_short(self):
        with pytest.raises(ValueError):
            validate_phone("12345")

    def test_invalid_phone_letters(self):
        with pytest.raises(ValueError):
            validate_phone("abcdefghij")

    def test_valid_international_phone(self):
        result = validate_phone("+442071234567")
        assert result.startswith("+44")


class TestValidateStatus:

    def test_valid_status_active(self):
        allowed = ["active", "processing", "archived"]
        result = validate_status("active", allowed)
        assert result == "active"

    def test_valid_status_processing(self):
        allowed = ["active", "processing", "archived"]
        result = validate_status("processing", allowed)
        assert result == "processing"

    def test_valid_status_archived(self):
        allowed = ["active", "processing", "archived"]
        result = validate_status("archived", allowed)
        assert result == "archived"

    def test_invalid_status(self):
        allowed = ["active", "processing", "archived"]
        with pytest.raises(ValueError, match="Status must be one of"):
            validate_status("deleted", allowed)

    def test_invalid_status_empty(self):
        allowed = ["active", "processing", "archived"]
        with pytest.raises(ValueError, match="Status must be one of"):
            validate_status("", allowed)

    def test_status_case_sensitive(self):
        allowed = ["active", "processing", "archived"]
        with pytest.raises(ValueError, match="Status must be one of"):
            validate_status("Active", allowed)


class TestGenerateFoobarKey:

    def test_returns_string(self):
        key = generate_foobar_key()
        assert isinstance(key, str)

    def test_returns_sha1_length(self):
        key = generate_foobar_key()
        assert len(key) == 40

    def test_returns_hex_characters(self):
        key = generate_foobar_key()
        assert all(c in '0123456789abcdef' for c in key)

    def test_unique_keys(self):
        keys = {generate_foobar_key() for _ in range(100)}
        assert len(keys) == 100
