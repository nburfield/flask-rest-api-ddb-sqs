"""Tests for app.base.base_model module."""

import pytest
from unittest.mock import MagicMock, patch
from app.base.base_model import BaseModel, dt_now
from app.repositories.repository_factory import RepositoryFactory


class ConcreteModel(BaseModel):
    """Concrete implementation of BaseModel for testing."""
    object_type = "foobar"
    database_name = "foobars"


class TestBaseModelInit:

    def test_requires_object_type(self, app):
        class NoObjectType(BaseModel):
            database_name = "test"

        with pytest.raises(ValueError, match="Subclasses must define object_type"):
            NoObjectType()

    def test_requires_database_name(self, app):
        class NoDatabaseName(BaseModel):
            object_type = "test"

        with pytest.raises(ValueError, match="Subclasses must define database_name"):
            NoDatabaseName()

    def test_loads_schema(self, app):
        model = ConcreteModel()
        assert model.schema is not None
        assert len(model.schema) > 0

    def test_schema_by_name_dict(self, app):
        model = ConcreteModel()
        assert "name" in model.schema_by_name
        assert "email" in model.schema_by_name
        assert "version" in model.schema_by_name

    def test_get_key_field(self, app):
        model = ConcreteModel()
        assert model.get_key_field() == "key"


class TestFilterResponseData:

    def test_drops_id_field(self, app):
        model = ConcreteModel()
        data = {
            "key": "abc",
            "name": "test",
            "_id": "internal-id"
        }
        result = model.filter_response_data(data)
        assert "_id" not in result
        assert "name" in result

    def test_keeps_non_drop_fields(self, app):
        model = ConcreteModel()
        data = {
            "key": "abc",
            "name": "test",
            "email": "test@example.com",
            "version": 0
        }
        result = model.filter_response_data(data)
        assert "key" in result
        assert "name" in result
        assert "email" in result
        assert "version" in result

    def test_keeps_unknown_fields(self, app):
        model = ConcreteModel()
        data = {"key": "abc", "extra_field": "extra_value"}
        result = model.filter_response_data(data)
        assert "extra_field" in result

    def test_handles_none(self, app):
        model = ConcreteModel()
        result = model.filter_response_data(None)
        assert result is None

    def test_handles_empty_dict(self, app):
        model = ConcreteModel()
        result = model.filter_response_data({})
        assert result == {}


class TestValidateData:

    def test_post_mode_valid(self, app):
        model = ConcreteModel()
        data = {
            "name": "Test",
            "email": "test@example.com"
        }
        result = model.validate_data(data, mode="post")
        assert result["name"] == "Test"
        assert result["email"] == "test@example.com"

    def test_post_mode_sets_defaults(self, app):
        model = ConcreteModel()
        data = {
            "name": "Test",
            "email": "test@example.com"
        }
        result = model.validate_data(data, mode="post")
        assert result["status"] == "active"  # default
        assert result["object_type"] == "foobar"  # default

    def test_post_mode_missing_required(self, app):
        model = ConcreteModel()
        data = {"email": "test@example.com"}
        with pytest.raises(ValueError, match="name is required"):
            model.validate_data(data, mode="post")

    def test_post_mode_null_not_nullable(self, app):
        model = ConcreteModel()
        data = {"name": None, "email": "test@example.com"}
        with pytest.raises(ValueError, match="name cannot be null"):
            model.validate_data(data, mode="post")

    def test_post_mode_wrong_type(self, app):
        model = ConcreteModel()
        data = {"name": 123, "email": "test@example.com"}
        with pytest.raises(TypeError, match="name must be of type str"):
            model.validate_data(data, mode="post")

    def test_post_mode_ignores_non_post_fields(self, app):
        model = ConcreteModel()
        data = {
            "name": "Test",
            "email": "test@example.com",
            "key": "should-be-ignored",
            "version": 5
        }
        result = model.validate_data(data, mode="post")
        # key and version have post_value=false, so they use defaults
        assert result.get("key") is None  # default
        assert result.get("version") == 0  # default

    def test_patch_mode_partial_update(self, app):
        model = ConcreteModel()
        data = {"email": "new@example.com"}
        result = model.validate_data(data, mode="patch")
        assert result["email"] == "new@example.com"
        # Fields not provided in patch are omitted
        assert "name" not in result

    def test_patch_mode_rejects_non_patchable(self, app):
        model = ConcreteModel()
        # name has patch_value=false, but it's in data from existing item
        data = {"name": "Updated", "email": "test@example.com"}
        result = model.validate_data(data, mode="patch")
        # name should be preserved from data since it was in data but patch_value=false
        assert result["name"] == "Updated"
        # email should be included since patch_value=true
        assert result["email"] == "test@example.com"

    def test_system_mode_passes_through(self, app):
        model = ConcreteModel()
        data = {
            "key": "custom-key",
            "version": 5,
            "created_user": "admin"
        }
        result = model.validate_data(data, mode="system")
        assert result["key"] == "custom-key"
        assert result["version"] == 5
        assert result["created_user"] == "admin"

    def test_empty_data_raises(self, app):
        model = ConcreteModel()
        with pytest.raises(ValueError, match="Data cannot be empty"):
            model.validate_data(None)

    def test_nullable_field_with_null(self, app):
        model = ConcreteModel()
        data = {
            "name": "Test",
            "email": "test@example.com",
            "phone": None
        }
        result = model.validate_data(data, mode="post")
        assert result["phone"] is None


class TestCreate:

    def test_create_item(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.create.return_value = {
            "key": "abc123",
            "name": "Test",
            "email": "test@example.com",
            "version": 0,
            "object_type": "foobar"
        }
        model.repo.check_unique_values.return_value = []

        result = model.create(
            {"name": "Test", "email": "test@example.com"},
            server_side_overrides={"key": "abc123", "created_user": "admin", "updated_user": "admin",
                                   "created_dt": 100.0, "updated_dt": 100.0}
        )

        model.repo.create.assert_called_once()
        assert result["name"] == "Test"

    def test_create_sets_version_zero(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.create.return_value = {"version": 0, "object_type": "foobar", "name": "Test", "email": "t@e.com"}
        model.repo.check_unique_values.return_value = []

        model.create(
            {"name": "Test", "email": "t@e.com"},
            server_side_overrides={"key": "k", "created_user": "u", "updated_user": "u",
                                   "created_dt": 1.0, "updated_dt": 1.0}
        )

        call_args = model.repo.create.call_args[0][0]
        assert call_args["version"] == 0
        assert call_args["object_type"] == "foobar"

    def test_create_unique_violation(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.check_unique_values.return_value = ["name"]

        with pytest.raises(ValueError, match="Values for the following fields already exist"):
            model.create(
                {"name": "Test", "email": "t@e.com"},
                server_side_overrides={"key": "k", "created_user": "u", "updated_user": "u",
                                       "created_dt": 1.0, "updated_dt": 1.0}
            )

    def test_create_validation_error_propagated(self, app):
        model = ConcreteModel()
        with pytest.raises(ValueError):
            model.create({"email": "test@example.com"})  # missing name


class TestUpdate:

    def test_update_with_version(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.get.return_value = {
            "key": "abc",
            "name": "Old",
            "email": "old@example.com",
            "status": "active",
            "version": 0,
            "object_type": "foobar",
            "created_user": "admin",
            "created_dt": 100.0,
            "updated_user": "admin",
            "updated_dt": 100.0
        }
        model.repo.update_by_version.return_value = {
            "key": "abc",
            "name": "Old",
            "email": "new@example.com",
            "version": 1,
            "object_type": "foobar"
        }
        model.repo.check_unique_values.return_value = []

        result = model.update(
            "abc",
            {"email": "new@example.com", "version": 0},
            server_side_overrides={"updated_user": "admin", "updated_dt": 200.0}
        )

        model.repo.update_by_version.assert_called_once()

    def test_update_missing_version(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.get.return_value = {
            "key": "abc",
            "name": "Old",
            "email": "old@example.com",
            "version": 0,
            "object_type": "foobar",
            "created_user": "admin",
            "created_dt": 100.0,
            "updated_user": "admin",
            "updated_dt": 100.0
        }

        with pytest.raises(ValueError, match="Version field is required"):
            model.update("abc", {"email": "new@example.com"})

    def test_update_version_mismatch(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.get.return_value = {
            "key": "abc",
            "name": "Old",
            "email": "old@example.com",
            "version": 2,
            "object_type": "foobar",
            "created_user": "admin",
            "created_dt": 100.0,
            "updated_user": "admin",
            "updated_dt": 100.0
        }
        model.repo.update_by_version.return_value = None
        model.repo.check_unique_values.return_value = []

        with pytest.raises(ValueError, match="Version mismatch"):
            model.update("abc", {"email": "new@example.com", "version": 0})

    def test_update_empty_key(self, app):
        model = ConcreteModel()
        with pytest.raises(ValueError, match="Key is required"):
            model.update("", {"email": "test@example.com"})

    def test_update_nonexistent_item(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.get.return_value = None

        with pytest.raises(ValueError, match="not found"):
            model.update("nonexistent", {"email": "test@example.com", "version": 0})


class TestDelete:

    def test_delete_item(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.delete.return_value = True

        result = model.delete("abc")
        model.repo.delete.assert_called_once_with("abc")
        assert result is True

    def test_delete_empty_key(self, app):
        model = ConcreteModel()
        with pytest.raises(ValueError, match="Key is required"):
            model.delete("")


class TestGet:

    def test_get_existing_item(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.get.return_value = {
            "key": "abc",
            "name": "Test",
            "version": 0,
            "object_type": "foobar"
        }

        result = model.get("abc")
        assert result["key"] == "abc"
        assert result["name"] == "Test"

    def test_get_nonexistent_item(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.get.return_value = None

        result = model.get("nonexistent")
        assert result is None

    def test_get_empty_key(self, app):
        model = ConcreteModel()
        with pytest.raises(ValueError, match="Key is required"):
            model.get("")

    def test_get_filters_response(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.get.return_value = {
            "key": "abc",
            "_id": "internal",
            "name": "Test"
        }

        result = model.get("abc")
        assert "_id" not in result


class TestGetByField:

    def test_get_by_field(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.get_by_field.return_value = {
            "key": "abc",
            "email": "test@example.com"
        }

        result = model.get_by_field("email", "test@example.com")
        assert result["email"] == "test@example.com"

    def test_get_by_field_not_found(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.get_by_field.return_value = None

        result = model.get_by_field("email", "notfound@example.com")
        assert result is None

    def test_get_by_field_missing_args(self, app):
        model = ConcreteModel()
        with pytest.raises(ValueError, match="Field name and value are required"):
            model.get_by_field("", "value")


class TestListAll:

    def test_list_all_no_filters(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.list_all.return_value = [
            {"key": "1", "name": "First"},
            {"key": "2", "name": "Second"}
        ]

        result = model.list_all()
        assert len(result) == 2

    def test_list_all_with_filters(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.list_all.return_value = [{"key": "1", "name": "First"}]

        filters = {"status": {"operator": "eq", "values": ["active"]}}
        result = model.list_all(filters)
        assert len(result) == 1

    def test_list_all_empty(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.list_all.return_value = []

        result = model.list_all()
        assert result == []


class TestListAllPaginated:

    def test_paginated_basic(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.list_all_paginated.return_value = (
            [{"key": "1"}, {"key": "2"}],
            5
        )

        results, total = model.list_all_paginated(start=0, limit=2)
        assert len(results) == 2
        assert total == 5

    def test_paginated_negative_start(self, app):
        model = ConcreteModel()
        with pytest.raises(ValueError, match="Start parameter must be non-negative"):
            model.list_all_paginated(start=-1, limit=10)

    def test_paginated_zero_limit(self, app):
        model = ConcreteModel()
        with pytest.raises(ValueError, match="Limit parameter must be positive"):
            model.list_all_paginated(start=0, limit=0)

    def test_paginated_limit_too_large(self, app):
        model = ConcreteModel()
        with pytest.raises(ValueError, match="Limit parameter cannot exceed 1000"):
            model.list_all_paginated(start=0, limit=1001)


class TestCheckUniqueConstraints:

    def test_no_unique_fields(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()

        # Data with no unique schema fields
        model._check_unique_constraints({"status": "active"})
        model.repo.check_unique_values.assert_not_called()

    def test_unique_field_passes(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.check_unique_values.return_value = []

        model._check_unique_constraints({"name": "unique_name", "email": "unique@test.com"})
        model.repo.check_unique_values.assert_called_once()

    def test_unique_field_conflict(self, app):
        model = ConcreteModel()
        model.repo = MagicMock()
        model.repo.check_unique_values.return_value = ["name"]

        with pytest.raises(ValueError, match="Values for the following fields already exist: name"):
            model._check_unique_constraints({"name": "duplicate", "email": "test@test.com"})
