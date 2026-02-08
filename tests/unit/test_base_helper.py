"""Tests for app.base.base_helper module."""

import pytest
from unittest.mock import MagicMock, patch
from flask import Flask

from app.base.base_helper import BaseHelper
from app.models.foobar import FoobarModel


class TestBaseHelperGetAll:

    def test_get_all_returns_200(self, app):
        from werkzeug.datastructures import MultiDict
        with app.test_request_context('/api/v2/foobars'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.list_all_paginated.return_value = (
                [{"key": "1", "name": "Test"}],
                1
            )
            helper.model.schema_by_name = FoobarModel.__dict__.get('schema_by_name', {})

            response, status_code = helper.get_all(query_params=MultiDict())
            assert status_code == 200

    def test_get_all_empty_list(self, app):
        from werkzeug.datastructures import MultiDict
        with app.test_request_context('/api/v2/foobars'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.list_all_paginated.return_value = ([], 0)

            response, status_code = helper.get_all(query_params=MultiDict())
            assert status_code == 200

            data = response.get_json()
            assert data["size"] == 0
            assert data["total_count"] == 0
            assert data["values"] == []

    def test_get_all_with_pagination(self, app):
        from werkzeug.datastructures import MultiDict
        query_params = MultiDict([('start', '0'), ('limit', '2')])

        with app.test_request_context('/api/v2/foobars?start=0&limit=2'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.list_all_paginated.return_value = (
                [{"key": "1"}, {"key": "2"}],
                5
            )

            response, status_code = helper.get_all(query_params=query_params)
            data = response.get_json()

            assert data["isLastPage"] is False
            assert data["nextPageStart"] == 2

    def test_get_all_with_filters(self, app):
        from werkzeug.datastructures import MultiDict
        query_params = MultiDict([('status', 'active')])

        with app.test_request_context('/api/v2/foobars?status=active'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.list_all_paginated.return_value = ([{"key": "1"}], 1)

            response, status_code = helper.get_all(query_params=query_params)
            assert status_code == 200


class TestBaseHelperGetByKey:

    def test_get_existing_item(self, app):
        with app.test_request_context('/api/v2/foobars/abc123'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.get.return_value = {"key": "abc123", "name": "Test"}

            response, status_code = helper.get_by_key("abc123")
            assert status_code == 200

            data = response.get_json()
            assert data["key"] == "abc123"

    def test_get_nonexistent_item_aborts_404(self, app):
        with app.test_request_context('/api/v2/foobars/nonexistent'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.get.return_value = None

            with pytest.raises(Exception):
                helper.get_by_key("nonexistent")


class TestBaseHelperCreate:

    def test_create_returns_201(self, app):
        with app.test_request_context('/api/v2/foobars'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.schema_by_name = {
                "created_dt": {"default": lambda: 100.0},
                "updated_dt": {"default": lambda: 100.0}
            }
            helper.model.create.return_value = {
                "key": "abc",
                "name": "Test",
                "version": 0
            }

            response, status_code = helper.create(
                {"name": "Test", "email": "test@example.com"},
                "testuser"
            )
            assert status_code == 201

    def test_create_empty_data_aborts_400(self, app):
        with app.test_request_context('/api/v2/foobars'):
            helper = BaseHelper(FoobarModel)
            with pytest.raises(Exception):
                helper.create(None, "testuser")

    def test_create_validation_error_aborts_400(self, app):
        with app.test_request_context('/api/v2/foobars'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.schema_by_name = {
                "created_dt": {"default": lambda: 100.0},
                "updated_dt": {"default": lambda: 100.0}
            }
            helper.model.create.side_effect = ValueError("name is required")

            with pytest.raises(Exception):
                helper.create({"email": "test@example.com"}, "testuser")


class TestBaseHelperUpdate:

    def test_update_returns_200(self, app):
        with app.test_request_context('/api/v2/foobars/abc'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.schema_by_name = {
                "updated_dt": {"default": lambda: 200.0}
            }
            helper.model.get.return_value = {"key": "abc", "version": 0}
            helper.model.update.return_value = {
                "key": "abc",
                "email": "new@example.com",
                "version": 1
            }

            response, status_code = helper.update(
                "abc",
                {"email": "new@example.com", "version": 0},
                "testuser"
            )
            assert status_code == 200

    def test_update_nonexistent_aborts_404(self, app):
        with app.test_request_context('/api/v2/foobars/nonexistent'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.schema_by_name = {
                "updated_dt": {"default": lambda: 200.0}
            }
            helper.model.get.return_value = None

            with pytest.raises(Exception):
                helper.update("nonexistent", {"email": "t@e.com", "version": 0}, "testuser")

    def test_update_version_conflict_aborts_409(self, app):
        with app.test_request_context('/api/v2/foobars/abc'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.schema_by_name = {
                "updated_dt": {"default": lambda: 200.0}
            }
            helper.model.get.return_value = {"key": "abc", "version": 2}
            helper.model.update.side_effect = ValueError("Version mismatch. Expected version 2, but received 0")

            with pytest.raises(Exception):
                helper.update("abc", {"email": "t@e.com", "version": 0}, "testuser")

    def test_update_empty_key_aborts_400(self, app):
        with app.test_request_context('/api/v2/foobars/'):
            helper = BaseHelper(FoobarModel)
            with pytest.raises(Exception):
                helper.update("", {"email": "t@e.com"}, "testuser")

    def test_update_empty_data_aborts_400(self, app):
        with app.test_request_context('/api/v2/foobars/abc'):
            helper = BaseHelper(FoobarModel)
            with pytest.raises(Exception):
                helper.update("abc", None, "testuser")

    def test_update_missing_version_aborts_400(self, app):
        with app.test_request_context('/api/v2/foobars/abc'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.schema_by_name = {
                "updated_dt": {"default": lambda: 200.0}
            }
            helper.model.get.return_value = {"key": "abc", "version": 0}
            helper.model.update.side_effect = ValueError("Version field is required for updates")

            with pytest.raises(Exception):
                helper.update("abc", {"email": "t@e.com"}, "testuser")


class TestBaseHelperDelete:

    def test_delete_returns_204(self, app):
        with app.test_request_context('/api/v2/foobars/abc'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.get.return_value = {"key": "abc"}
            helper.model.delete.return_value = True

            response, status_code = helper.delete("abc")
            assert status_code == 204

    def test_delete_nonexistent_aborts_404(self, app):
        with app.test_request_context('/api/v2/foobars/nonexistent'):
            helper = BaseHelper(FoobarModel)
            helper.model = MagicMock()
            helper.model.get.return_value = None

            with pytest.raises(Exception):
                helper.delete("nonexistent")

    def test_delete_empty_key_aborts_400(self, app):
        with app.test_request_context('/api/v2/foobars/'):
            helper = BaseHelper(FoobarModel)
            with pytest.raises(Exception):
                helper.delete("")
