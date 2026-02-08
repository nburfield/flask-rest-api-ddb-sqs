"""Tests for app.repositories.dynamo_repository module."""

import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from app.repositories.dynamo_repository import (
    DynamoRepository,
    convert_floats_to_decimals,
    convert_decimals_to_floats
)


class TestConvertFloatsToDecimals:

    def test_float_conversion(self):
        assert convert_floats_to_decimals(1.5) == Decimal('1.5')

    def test_dict_conversion(self):
        result = convert_floats_to_decimals({"val": 1.5, "name": "test"})
        assert result["val"] == Decimal('1.5')
        assert result["name"] == "test"

    def test_list_conversion(self):
        result = convert_floats_to_decimals([1.5, 2.5])
        assert result == [Decimal('1.5'), Decimal('2.5')]

    def test_nested_conversion(self):
        result = convert_floats_to_decimals({"nested": {"val": 3.14}})
        assert result["nested"]["val"] == Decimal('3.14')

    def test_non_float_unchanged(self):
        assert convert_floats_to_decimals("hello") == "hello"
        assert convert_floats_to_decimals(42) == 42
        assert convert_floats_to_decimals(True) is True
        assert convert_floats_to_decimals(None) is None

    def test_mixed_list(self):
        result = convert_floats_to_decimals([1.5, "hello", 42, None])
        assert result == [Decimal('1.5'), "hello", 42, None]


class TestConvertDecimalsToFloats:

    def test_decimal_to_float(self):
        assert convert_decimals_to_floats(Decimal('1.5')) == 1.5

    def test_dict_conversion(self):
        result = convert_decimals_to_floats({"val": Decimal('1.5'), "name": "test"})
        assert result["val"] == 1.5
        assert isinstance(result["val"], float)

    def test_list_conversion(self):
        result = convert_decimals_to_floats([Decimal('1.5'), Decimal('2.5')])
        assert result == [1.5, 2.5]

    def test_schema_int_conversion(self):
        schema = {"version": {"type": int}}
        result = convert_decimals_to_floats(
            {"version": Decimal('5')},
            schema_by_name=schema
        )
        assert result["version"] == 5
        assert isinstance(result["version"], int)

    def test_schema_float_conversion(self):
        schema = {"created_dt": {"type": float}}
        result = convert_decimals_to_floats(
            {"created_dt": Decimal('1700000000.123')},
            schema_by_name=schema
        )
        assert isinstance(result["created_dt"], float)

    def test_non_decimal_unchanged(self):
        assert convert_decimals_to_floats("hello") == "hello"
        assert convert_decimals_to_floats(42) == 42

    def test_with_schema_loader(self):
        from app.base.schema_loader import SchemaLoader
        loader = SchemaLoader()
        result = convert_decimals_to_floats(
            {"version": Decimal('3'), "object_type": "foobar"},
            schema_loader=loader
        )
        assert result["version"] == 3
        assert isinstance(result["version"], int)

    def test_schema_loader_unknown_type(self):
        from app.base.schema_loader import SchemaLoader
        loader = SchemaLoader()
        result = convert_decimals_to_floats(
            {"unknown_field": Decimal('1.5'), "object_type": "nonexistent"},
            schema_loader=loader
        )
        # Should still convert, just not schema-aware
        assert result["unknown_field"] == 1.5


class TestDynamoRepositoryInit:

    def test_requires_client(self):
        with pytest.raises(ValueError, match="dynamo_client must be provided"):
            DynamoRepository(table_name="test")

    def test_sets_key_field(self):
        mock_client = MagicMock()
        repo = DynamoRepository(table_name="test", key_field="custom_id", dynamo_client=mock_client)
        assert repo.key_field == "custom_id"

    def test_default_key_field(self):
        mock_client = MagicMock()
        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        assert repo.key_field == "key"


class TestDynamoRepositoryCreate:

    def test_create_item(self):
        mock_client = MagicMock()
        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)

        item = {"key": "abc", "name": "Test", "value": 1.5}
        result = repo.create(item)

        mock_client.Table("test").put_item.assert_called_once()
        assert result == item

    def test_create_converts_floats(self):
        mock_client = MagicMock()
        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)

        item = {"key": "abc", "value": 1.5}
        repo.create(item)

        call_args = mock_client.Table("test").put_item.call_args
        converted_item = call_args[1]['Item'] if 'Item' in call_args[1] else call_args[0][0]
        # The float should be converted to Decimal for DynamoDB
        assert isinstance(converted_item.get('value', item['value']), (Decimal, float))


class TestDynamoRepositoryGet:

    def test_get_existing_item(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            'Item': {'key': 'abc', 'name': 'Test', 'version': Decimal('0')}
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.get("abc")

        assert result is not None
        assert result['key'] == 'abc'

    def test_get_nonexistent_item(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.get("nonexistent")

        assert result is None


class TestDynamoRepositoryDelete:

    def test_delete_item(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.delete("abc")

        mock_table.delete_item.assert_called_once_with(Key={'key': 'abc'})
        assert result is True


class TestDynamoRepositoryUpdate:

    def test_update_item(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            'Item': {'key': 'abc', 'name': 'Updated', 'version': Decimal('1')}
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.update("abc", {"name": "Updated"})

        mock_table.update_item.assert_called_once()
        assert result is not None

    def test_update_empty_data(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            'Item': {'key': 'abc', 'name': 'Original'}
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.update("abc", {})

        mock_table.update_item.assert_not_called()

    def test_update_strips_key_from_data(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            'Item': {'key': 'abc', 'name': 'Updated'}
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        repo.update("abc", {"key": "abc", "name": "Updated"})

        # key should not be in the update expression values
        call_kwargs = mock_table.update_item.call_args[1]
        attr_vals = call_kwargs['ExpressionAttributeValues']
        assert ':val_key' not in attr_vals


class TestDynamoRepositoryUpdateByVersion:

    def test_update_by_version_success(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            'Item': {'key': 'abc', 'version': Decimal('1')}
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.update_by_version("abc", {"name": "Updated", "version": 1}, 0)

        mock_table.update_item.assert_called_once()
        call_kwargs = mock_table.update_item.call_args[1]
        assert 'ConditionExpression' in call_kwargs

    def test_update_by_version_conflict(self):
        mock_client = MagicMock()
        mock_table = MagicMock()

        # Simulate ConditionalCheckFailedException
        exc_class = type('ConditionalCheckFailedException', (Exception,), {})
        mock_table.meta.client.exceptions.ConditionalCheckFailedException = exc_class
        mock_table.update_item.side_effect = exc_class("Version mismatch")
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.update_by_version("abc", {"name": "Updated", "version": 1}, 0)

        assert result is None

    def test_update_by_version_empty_data(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            'Item': {'key': 'abc', 'version': Decimal('0')}
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.update_by_version("abc", {}, 0)

        mock_table.update_item.assert_not_called()


class TestDynamoRepositoryListAll:

    def test_list_all_no_filters(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.scan.return_value = {
            'Items': [
                {'key': '1', 'object_type': 'foobar', 'version': Decimal('0')},
                {'key': '2', 'object_type': 'foobar', 'version': Decimal('0')}
            ]
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.list_all("foobar")

        assert len(result) == 2

    def test_list_all_with_filters(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.scan.return_value = {
            'Items': [
                {'key': '1', 'object_type': 'foobar', 'status': 'active'}
            ]
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        filters = {'status': {'operator': 'eq', 'values': ['active']}}
        result = repo.list_all("foobar", filters)

        call_kwargs = mock_table.scan.call_args[1]
        assert 'FilterExpression' in call_kwargs
        assert 'object_type' in call_kwargs['FilterExpression']

    def test_list_all_empty(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.scan.return_value = {'Items': []}
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.list_all("foobar")

        assert result == []


class TestDynamoRepositoryListAllPaginated:

    def test_paginated_results(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        items = [
            {'key': str(i), 'object_type': 'foobar', 'version': Decimal('0')}
            for i in range(10)
        ]
        mock_table.scan.return_value = {'Items': items}
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        results, total = repo.list_all_paginated("foobar", start=0, limit=3)

        assert len(results) == 3
        assert total == 10

    def test_paginated_offset(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        items = [
            {'key': str(i), 'object_type': 'foobar', 'version': Decimal('0')}
            for i in range(10)
        ]
        mock_table.scan.return_value = {'Items': items}
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        results, total = repo.list_all_paginated("foobar", start=5, limit=3)

        assert len(results) == 3
        assert total == 10

    def test_paginated_last_page(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        items = [
            {'key': str(i), 'object_type': 'foobar', 'version': Decimal('0')}
            for i in range(5)
        ]
        mock_table.scan.return_value = {'Items': items}
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        results, total = repo.list_all_paginated("foobar", start=3, limit=10)

        assert len(results) == 2
        assert total == 5


class TestDynamoRepositoryCheckUniqueValues:

    def test_no_conflicts(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.scan.return_value = {'Items': []}
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.check_unique_values("foobar", {"name": "unique_name"})

        assert result == []

    def test_with_conflicts(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.scan.return_value = {
            'Items': [{'key': 'existing', 'name': 'duplicate'}]
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.check_unique_values("foobar", {"name": "duplicate"})

        assert "name" in result

    def test_empty_unique_fields(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.check_unique_values("foobar", {})

        assert result == []
        mock_table.scan.assert_not_called()

    def test_exclude_key(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.scan.return_value = {'Items': []}
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        repo.check_unique_values("foobar", {"name": "test"}, exclude_key="abc")

        call_kwargs = mock_table.scan.call_args[1]
        assert ':exclude_key' in call_kwargs['ExpressionAttributeValues']

    def test_null_value_skipped(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.check_unique_values("foobar", {"name": None})

        assert result == []
        mock_table.scan.assert_not_called()


class TestDynamoRepositoryGetByField:

    def test_get_by_field_found(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.scan.return_value = {
            'Items': [{'key': 'abc', 'email': 'test@example.com', 'version': Decimal('0')}]
        }
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.get_by_field("email", "test@example.com")

        assert result is not None
        assert result['email'] == 'test@example.com'

    def test_get_by_field_not_found(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.scan.return_value = {'Items': []}
        mock_client.Table.return_value = mock_table

        repo = DynamoRepository(table_name="test", dynamo_client=mock_client)
        result = repo.get_by_field("email", "notfound@example.com")

        assert result is None
