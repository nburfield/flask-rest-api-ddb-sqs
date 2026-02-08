"""Tests for app.helpers.query_parser module."""

import pytest
from decimal import Decimal
from datetime import datetime
from werkzeug.datastructures import MultiDict

from app.helpers.query_parser import (
    QueryParser,
    build_mongo_filter,
    build_dynamo_filter
)


class TestQueryParserParseQueryParams:

    def test_simple_equality(self):
        args = MultiDict([('status', 'active')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert 'status' in filters
        assert filters['status']['operator'] == 'eq'
        assert filters['status']['values'] == ['active']
        assert filter_str == 'status=active'

    def test_multiple_values_same_key(self):
        args = MultiDict([('status', 'active'), ('status', 'pending')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['status']['operator'] == 'eq'
        assert filters['status']['values'] == ['active', 'pending']
        assert 'status=active' in filter_str
        assert 'status=pending' in filter_str

    def test_gt_operator(self):
        args = MultiDict([('created_dt__gt', '100.0')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['created_dt']['operator'] == 'gt'
        assert filter_str == 'created_dt__gt=100.0'

    def test_gte_operator(self):
        args = MultiDict([('version__gte', '5')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['version']['operator'] == 'gte'

    def test_lt_operator(self):
        args = MultiDict([('created_dt__lt', '200.0')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['created_dt']['operator'] == 'lt'

    def test_lte_operator(self):
        args = MultiDict([('version__lte', '10')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['version']['operator'] == 'lte'

    def test_ne_operator(self):
        args = MultiDict([('status__ne', 'archived')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['status']['operator'] == 'ne'
        assert filters['status']['values'] == ['archived']

    def test_in_operator(self):
        args = MultiDict([('status__in', 'active,pending,processing')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['status']['operator'] == 'in'
        assert filters['status']['values'] == [['active', 'pending', 'processing']]

    def test_nin_operator(self):
        args = MultiDict([('status__nin', 'archived,deleted')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['status']['operator'] == 'nin'
        assert filters['status']['values'] == [['archived', 'deleted']]

    def test_contains_operator(self):
        args = MultiDict([('name__contains', 'test')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['name']['operator'] == 'contains'
        assert filters['name']['values'] == ['test']

    def test_regex_operator(self):
        args = MultiDict([('email__regex', '.*@example.com')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters['email']['operator'] == 'regex'

    def test_skip_pagination_params(self):
        args = MultiDict([('start', '0'), ('limit', '10'), ('status', 'active')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert 'start' not in filters
        assert 'limit' not in filters
        assert 'status' in filters

    def test_skip_page_per_page_params(self):
        args = MultiDict([('page', '1'), ('per_page', '20')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert 'page' not in filters
        assert 'per_page' not in filters

    def test_invalid_operator_treated_as_eq(self):
        args = MultiDict([('status__invalid', 'active')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert 'status__invalid' in filters
        assert filters['status__invalid']['operator'] == 'eq'

    def test_empty_query_params(self):
        args = MultiDict()
        filters, filter_str = QueryParser.parse_query_params(args)

        assert filters == {}
        assert filter_str == ""

    def test_multiple_filters(self):
        args = MultiDict([('status', 'active'), ('name', 'test')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert 'status' in filters
        assert 'name' in filters

    def test_datetime_parsing(self):
        args = MultiDict([('created_dt__gt', '2023-01-01')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert isinstance(filters['created_dt']['values'][0], datetime)

    def test_datetime_with_time_parsing(self):
        args = MultiDict([('created_dt__gt', '2023-01-01T12:00:00')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert isinstance(filters['created_dt']['values'][0], datetime)

    def test_float_parsing(self):
        args = MultiDict([('created_dt__gt', '1700000000.5')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert isinstance(filters['created_dt']['values'][0], float)

    def test_int_parsing(self):
        args = MultiDict([('version__gt', '5')])
        filters, filter_str = QueryParser.parse_query_params(args)

        assert isinstance(filters['version']['values'][0], int)


class TestBuildMongoFilter:

    def test_eq_single_value(self):
        parsed = {'status': {'operator': 'eq', 'values': ['active']}}
        result = build_mongo_filter(parsed)
        assert result == {'status': 'active'}

    def test_eq_multiple_values(self):
        parsed = {'status': {'operator': 'eq', 'values': ['active', 'pending']}}
        result = build_mongo_filter(parsed)
        assert result == {'status': {'$in': ['active', 'pending']}}

    def test_ne(self):
        parsed = {'status': {'operator': 'ne', 'values': ['archived']}}
        result = build_mongo_filter(parsed)
        assert result == {'status': {'$ne': 'archived'}}

    def test_gt(self):
        parsed = {'version': {'operator': 'gt', 'values': [5]}}
        result = build_mongo_filter(parsed)
        assert result == {'version': {'$gt': 5}}

    def test_gte(self):
        parsed = {'version': {'operator': 'gte', 'values': [5]}}
        result = build_mongo_filter(parsed)
        assert result == {'version': {'$gte': 5}}

    def test_lt(self):
        parsed = {'version': {'operator': 'lt', 'values': [10]}}
        result = build_mongo_filter(parsed)
        assert result == {'version': {'$lt': 10}}

    def test_lte(self):
        parsed = {'version': {'operator': 'lte', 'values': [10]}}
        result = build_mongo_filter(parsed)
        assert result == {'version': {'$lte': 10}}

    def test_in(self):
        parsed = {'status': {'operator': 'in', 'values': [['active', 'pending']]}}
        result = build_mongo_filter(parsed)
        assert result == {'status': {'$in': ['active', 'pending']}}

    def test_nin(self):
        parsed = {'status': {'operator': 'nin', 'values': [['archived']]}}
        result = build_mongo_filter(parsed)
        assert result == {'status': {'$nin': ['archived']}}

    def test_contains(self):
        parsed = {'name': {'operator': 'contains', 'values': ['test']}}
        result = build_mongo_filter(parsed)
        assert result == {'name': {'$regex': 'test', '$options': 'i'}}

    def test_regex(self):
        parsed = {'email': {'operator': 'regex', 'values': ['.*@example']}}
        result = build_mongo_filter(parsed)
        assert result == {'email': {'$regex': '.*@example'}}

    def test_empty_filters(self):
        result = build_mongo_filter({})
        assert result == {}


class TestBuildDynamoFilter:

    def test_eq_single_value(self):
        parsed = {'status': {'operator': 'eq', 'values': ['active']}}
        result = build_dynamo_filter(parsed)

        assert 'FilterExpression' in result
        assert '#status = :status' in result['FilterExpression']
        assert result['ExpressionAttributeValues'][':status'] == 'active'
        assert result['ExpressionAttributeNames']['#status'] == 'status'

    def test_eq_multiple_values_in(self):
        parsed = {'status': {'operator': 'eq', 'values': ['active', 'pending']}}
        result = build_dynamo_filter(parsed)

        assert 'IN' in result['FilterExpression']

    def test_ne(self):
        parsed = {'status': {'operator': 'ne', 'values': ['archived']}}
        result = build_dynamo_filter(parsed)

        assert '<>' in result['FilterExpression']

    def test_gt(self):
        parsed = {'version': {'operator': 'gt', 'values': [5]}}
        result = build_dynamo_filter(parsed)

        assert '>' in result['FilterExpression']

    def test_gte(self):
        parsed = {'version': {'operator': 'gte', 'values': [5]}}
        result = build_dynamo_filter(parsed)

        assert '>=' in result['FilterExpression']

    def test_lt(self):
        parsed = {'version': {'operator': 'lt', 'values': [10]}}
        result = build_dynamo_filter(parsed)

        assert '<' in result['FilterExpression']

    def test_lte(self):
        parsed = {'version': {'operator': 'lte', 'values': [10]}}
        result = build_dynamo_filter(parsed)

        assert '<=' in result['FilterExpression']

    def test_contains(self):
        parsed = {'name': {'operator': 'contains', 'values': ['test']}}
        result = build_dynamo_filter(parsed)

        assert 'contains(' in result['FilterExpression']

    def test_empty_filters(self):
        result = build_dynamo_filter({})
        assert result == {}

    def test_float_to_decimal_conversion(self):
        parsed = {'created_dt': {'operator': 'gt', 'values': [1700000000.5]}}
        result = build_dynamo_filter(parsed)

        val = result['ExpressionAttributeValues'][':created_dt']
        assert isinstance(val, Decimal)

    def test_multiple_filters_combined_with_and(self):
        parsed = {
            'status': {'operator': 'eq', 'values': ['active']},
            'name': {'operator': 'contains', 'values': ['test']}
        }
        result = build_dynamo_filter(parsed)

        assert ' AND ' in result['FilterExpression']
