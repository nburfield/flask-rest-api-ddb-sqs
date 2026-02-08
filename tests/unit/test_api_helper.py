"""Tests for app.helpers.api_helper module."""

import json
import pytest
from werkzeug.datastructures import MultiDict

from app.helpers.api_helper import (
    make_list_api_response,
    make_api_message,
    get_arg_list,
    get_arg_value,
    get_arg_dict,
    get_start_limit
)


class TestMakeListApiResponse:

    def test_basic_response(self):
        values = [{"key": "1"}, {"key": "2"}]
        result = make_list_api_response(values, 0, 10, True, "", 2)

        assert result["size"] == 2
        assert result["total_count"] == 2
        assert result["limit"] == 10
        assert result["isLastPage"] is True
        assert result["values"] == values
        assert result["start"] == 0
        assert result["filter"] == ""
        assert result["nextPageStart"] is None

    def test_pagination_not_last_page(self):
        values = [{"key": "1"}]
        result = make_list_api_response(values, 0, 1, False, "", 5)

        assert result["isLastPage"] is False
        assert result["nextPageStart"] == 1
        assert result["total_count"] == 5

    def test_pagination_second_page(self):
        values = [{"key": "2"}]
        result = make_list_api_response(values, 1, 1, False, "", 5)

        assert result["start"] == 1
        assert result["nextPageStart"] == 2

    def test_last_page(self):
        values = [{"key": "5"}]
        result = make_list_api_response(values, 4, 1, True, "", 5)

        assert result["isLastPage"] is True
        assert result["nextPageStart"] is None

    def test_empty_values(self):
        result = make_list_api_response([], 0, 10, True, "", 0)

        assert result["size"] == 0
        assert result["total_count"] == 0
        assert result["values"] == []

    def test_filter_string(self):
        result = make_list_api_response([], 0, 10, True, "status=active", 0)
        assert result["filter"] == "status=active"

    def test_combined_filter_string(self):
        result = make_list_api_response([], 0, 10, True, "status=active&name=test", 0)
        assert result["filter"] == "status=active&name=test"


class TestMakeApiMessage:

    def test_success_message(self):
        result = make_api_message("success", "Operation completed")
        assert result["status"] == "success"
        assert result["message"] == "Operation completed"

    def test_error_message(self):
        result = make_api_message("error", "Something went wrong")
        assert result["status"] == "error"
        assert result["message"] == "Something went wrong"

    def test_numeric_status(self):
        result = make_api_message(400, "Bad request")
        assert result["status"] == 400
        assert result["message"] == "Bad request"


class TestGetArgList:

    def test_key_present(self):
        args = MultiDict([('roles', 'admin'), ('roles', 'user')])
        values, filter_str = get_arg_list(args, 'roles', [], None)

        assert values == ['admin', 'user']
        assert 'roles=admin' in filter_str
        assert 'roles=user' in filter_str

    def test_key_not_present(self):
        args = MultiDict()
        values, filter_str = get_arg_list(args, 'roles', ['default'], None)

        assert values == ['default']
        assert filter_str is None

    def test_with_existing_filter(self):
        args = MultiDict([('status', 'active')])
        values, filter_str = get_arg_list(args, 'status', [], "name=test")

        assert values == ['active']
        assert 'name=test' in filter_str
        assert 'status=active' in filter_str


class TestGetArgValue:

    def test_key_present(self):
        args = MultiDict([('name', 'test')])
        value, filter_str = get_arg_value(args, 'name', None, None)

        assert value == 'test'
        assert filter_str == 'name=test'

    def test_key_not_present(self):
        args = MultiDict()
        value, filter_str = get_arg_value(args, 'name', 'default', None)

        assert value == 'default'
        assert filter_str is None

    def test_with_existing_filter(self):
        args = MultiDict([('name', 'test')])
        value, filter_str = get_arg_value(args, 'name', None, "status=active")

        assert value == 'test'
        assert 'status=active' in filter_str
        assert 'name=test' in filter_str


class TestGetArgDict:

    def test_valid_json_string(self):
        args = MultiDict([('data', '{"key": "value"}')])
        value, filter_str = get_arg_dict(args, 'data', None, None)

        assert value == {"key": "value"}
        assert 'data=' in filter_str

    def test_key_not_present(self):
        args = MultiDict()
        value, filter_str = get_arg_dict(args, 'data', {}, None)

        assert value == {}
        assert filter_str is None

    def test_invalid_json_raises(self):
        args = MultiDict([('data', 'not-json')])
        with pytest.raises(Exception, match="Could not convert"):
            get_arg_dict(args, 'data', None, None)


class TestGetStartLimit:

    def test_defaults(self):
        args = MultiDict()
        start, limit, filter_str = get_start_limit(
            args, start_default=0, limit_default=50, current_filter=None
        )

        assert start == 0
        assert limit == 50
        assert filter_str is None

    def test_custom_start(self):
        args = MultiDict([('start', '10')])
        start, limit, filter_str = get_start_limit(
            args, start_default=0, limit_default=50, current_filter=None
        )

        assert start == 10
        assert limit == 50
        assert 'start=10' in filter_str

    def test_custom_limit(self):
        args = MultiDict([('limit', '25')])
        start, limit, filter_str = get_start_limit(
            args, start_default=0, limit_default=50, current_filter=None
        )

        assert start == 0
        assert limit == 25
        assert 'limit=25' in filter_str

    def test_both_start_and_limit(self):
        args = MultiDict([('start', '5'), ('limit', '20')])
        start, limit, filter_str = get_start_limit(
            args, start_default=0, limit_default=50, current_filter=None
        )

        assert start == 5
        assert limit == 20
        assert 'start=5' in filter_str
        assert 'limit=20' in filter_str

    def test_with_existing_filter(self):
        args = MultiDict([('start', '5')])
        start, limit, filter_str = get_start_limit(
            args, start_default=0, limit_default=50, current_filter="status=active"
        )

        assert 'status=active' in filter_str
        assert 'start=5' in filter_str

    def test_invalid_start_raises(self):
        args = MultiDict([('start', 'abc')])
        with pytest.raises(Exception, match="Could not convert 'start'"):
            get_start_limit(
                args, start_default=0, limit_default=50, current_filter=None
            )

    def test_invalid_limit_raises(self):
        args = MultiDict([('limit', 'xyz')])
        with pytest.raises(Exception, match="Could not convert 'limit'"):
            get_start_limit(
                args, start_default=0, limit_default=50, current_filter=None
            )
