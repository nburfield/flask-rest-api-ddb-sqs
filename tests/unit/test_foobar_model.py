"""Tests for app.models.foobar module."""

import pytest
from unittest.mock import MagicMock
from app.models.foobar import FoobarModel


class TestFoobarModelInit:

    def test_object_type(self, app):
        model = FoobarModel()
        assert model.object_type == "foobar"

    def test_database_name(self, app):
        model = FoobarModel()
        assert model.database_name == "foobars"


class TestFoobarValidateData:

    def test_validates_email(self, app):
        model = FoobarModel()
        data = {
            "name": "Test",
            "email": "USER@EXAMPLE.COM",
            "status": "active"
        }
        result = model.validate_data(data, mode="post")
        assert result["email"] == "user@example.com"

    def test_validates_phone(self, app):
        model = FoobarModel()
        data = {
            "name": "Test",
            "email": "test@example.com",
            "phone": "(202) 555-1234",
            "status": "active"
        }
        result = model.validate_data(data, mode="post")
        assert result["phone"] == "+12025551234"

    def test_validates_status(self, app):
        model = FoobarModel()
        data = {
            "name": "Test",
            "email": "test@example.com",
            "status": "active"
        }
        result = model.validate_data(data, mode="post")
        assert result["status"] == "active"

    def test_invalid_email_rejected(self, app):
        model = FoobarModel()
        data = {
            "name": "Test",
            "email": "invalid-email",
            "status": "active"
        }
        with pytest.raises(ValueError, match="Invalid email"):
            model.validate_data(data, mode="post")

    def test_invalid_phone_rejected(self, app):
        model = FoobarModel()
        data = {
            "name": "Test",
            "email": "test@example.com",
            "phone": "12345",
            "status": "active"
        }
        with pytest.raises(ValueError):
            model.validate_data(data, mode="post")

    def test_invalid_status_rejected(self, app):
        model = FoobarModel()
        data = {
            "name": "Test",
            "email": "test@example.com",
            "status": "invalid_status"
        }
        with pytest.raises(ValueError, match="Status must be one of"):
            model.validate_data(data, mode="post")

    def test_null_phone_allowed(self, app):
        model = FoobarModel()
        data = {
            "name": "Test",
            "email": "test@example.com",
            "phone": None,
            "status": "active"
        }
        result = model.validate_data(data, mode="post")
        assert result["phone"] is None

    def test_patch_mode_validates_email(self, app):
        model = FoobarModel()
        data = {"email": "NEW@EXAMPLE.COM"}
        result = model.validate_data(data, mode="patch")
        assert result["email"] == "new@example.com"

    def test_patch_mode_validates_status(self, app):
        model = FoobarModel()
        data = {"status": "processing"}
        result = model.validate_data(data, mode="patch")
        assert result["status"] == "processing"

    def test_default_status_active(self, app):
        model = FoobarModel()
        data = {
            "name": "Test",
            "email": "test@example.com"
        }
        result = model.validate_data(data, mode="post")
        assert result["status"] == "active"


class TestFoobarCreate:

    def test_auto_generates_key(self, app):
        model = FoobarModel()
        model.repo = MagicMock()
        model.repo.create.return_value = {
            "key": "generated-key",
            "name": "Test",
            "email": "test@example.com",
            "version": 0,
            "object_type": "foobar"
        }
        model.repo.check_unique_values.return_value = []

        result = model.create(
            {"name": "Test", "email": "test@example.com"},
            server_side_overrides={
                "created_user": "admin",
                "updated_user": "admin",
                "created_dt": 100.0,
                "updated_dt": 100.0
            }
        )

        create_args = model.repo.create.call_args[0][0]
        assert "key" in create_args
        assert len(create_args["key"]) == 40  # SHA1 hex

    def test_preserves_existing_key(self, app):
        model = FoobarModel()
        model.repo = MagicMock()
        model.repo.create.return_value = {
            "key": "custom-key",
            "name": "Test",
            "email": "test@example.com",
            "version": 0,
            "object_type": "foobar"
        }
        model.repo.check_unique_values.return_value = []

        result = model.create(
            {"name": "Test", "email": "test@example.com", "key": "custom-key"},
            server_side_overrides={
                "created_user": "admin",
                "updated_user": "admin",
                "created_dt": 100.0,
                "updated_dt": 100.0
            }
        )

        create_args = model.repo.create.call_args[0][0]
        assert create_args["key"] == "custom-key"


class TestFoobarGetByField:

    def test_get_by_field_filtered(self, app):
        model = FoobarModel()
        model.repo = MagicMock()
        model.repo.get_by_field.return_value = {
            "key": "abc",
            "email": "test@example.com",
            "_id": "internal-id"
        }

        result = model.get_by_field("email", "test@example.com")
        assert "_id" not in result

    def test_get_by_field_unfiltered(self, app):
        model = FoobarModel()
        model.repo = MagicMock()
        model.repo.get_by_field.return_value = {
            "key": "abc",
            "email": "test@example.com",
            "_id": "internal-id"
        }

        result = model.get_by_field("email", "test@example.com", unfiltered=True)
        assert "_id" in result

    def test_get_by_field_not_found(self, app):
        model = FoobarModel()
        model.repo = MagicMock()
        model.repo.get_by_field.return_value = None

        result = model.get_by_field("email", "notfound@example.com")
        assert result is None

    def test_get_by_field_missing_args(self, app):
        model = FoobarModel()
        with pytest.raises(ValueError, match="Field name and value are required"):
            model.get_by_field("", "value")
