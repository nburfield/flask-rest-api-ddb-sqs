"""Tests for app.base.schema_loader module."""

import os
import json
import pytest
from app.base.schema_loader import SchemaLoader, TYPE_MAP, dt_now


class TestDtNow:

    def test_returns_float(self):
        result = dt_now()
        assert isinstance(result, float)

    def test_returns_positive_timestamp(self):
        result = dt_now()
        assert result > 0

    def test_returns_reasonable_timestamp(self):
        result = dt_now()
        # Should be a reasonable UTC timestamp (after year 2020)
        assert result > 1577836800  # 2020-01-01 UTC


class TestTypeMap:

    def test_str_mapping(self):
        assert TYPE_MAP["str"] is str

    def test_int_mapping(self):
        assert TYPE_MAP["int"] is int

    def test_float_mapping(self):
        assert TYPE_MAP["float"] is float

    def test_bool_mapping(self):
        assert TYPE_MAP["bool"] is bool

    def test_list_mapping(self):
        assert TYPE_MAP["list"] is list

    def test_dict_mapping(self):
        assert TYPE_MAP["dict"] is dict

    def test_dt_now_mapping(self):
        assert TYPE_MAP["dt_now"] is dt_now

    def test_null_mapping(self):
        assert TYPE_MAP["null"] is None


class TestSchemaLoader:

    def test_default_schema_dir(self):
        loader = SchemaLoader()
        assert loader.schema_dir.endswith('schemas')

    def test_custom_schema_dir(self):
        loader = SchemaLoader(schema_dir="/tmp/schemas")
        assert loader.schema_dir == "/tmp/schemas"

    def test_load_foobar_schema(self):
        loader = SchemaLoader()
        schema = loader.load_schema("foobar")

        assert isinstance(schema, list)
        assert len(schema) > 0

        field_names = [f["name"] for f in schema]
        assert "name" in field_names
        assert "email" in field_names
        assert "phone" in field_names
        assert "status" in field_names
        assert "key" in field_names
        assert "version" in field_names
        assert "object_type" in field_names

    def test_schema_type_conversion(self):
        loader = SchemaLoader()
        schema = loader.load_schema("foobar")

        schema_by_name = {f["name"]: f for f in schema}

        assert schema_by_name["name"]["type"] is str
        assert schema_by_name["email"]["type"] is str
        assert schema_by_name["version"]["type"] is int
        assert schema_by_name["created_dt"]["type"] is float

    def test_schema_default_conversion(self):
        loader = SchemaLoader()
        schema = loader.load_schema("foobar")

        schema_by_name = {f["name"]: f for f in schema}

        # dt_now default should be a callable function
        assert callable(schema_by_name["created_dt"]["default"])

        # version default should be 0
        assert schema_by_name["version"]["default"] == 0

        # foobar object_type default should be "foobar"
        assert schema_by_name["object_type"]["default"] == "foobar"

    def test_schema_null_fields(self):
        loader = SchemaLoader()
        schema = loader.load_schema("foobar")

        schema_by_name = {f["name"]: f for f in schema}

        assert schema_by_name["name"]["null"] is False
        assert schema_by_name["phone"]["null"] is True
        assert schema_by_name["status"]["null"] is True

    def test_schema_post_patch_flags(self):
        loader = SchemaLoader()
        schema = loader.load_schema("foobar")

        schema_by_name = {f["name"]: f for f in schema}

        # name: post_value=true, patch_value=false
        assert schema_by_name["name"]["post_value"] is True
        assert schema_by_name["name"]["patch_value"] is False

        # email: post_value=true, patch_value=true
        assert schema_by_name["email"]["post_value"] is True
        assert schema_by_name["email"]["patch_value"] is True

        # key: post_value=false, patch_value=false
        assert schema_by_name["key"]["post_value"] is False
        assert schema_by_name["key"]["patch_value"] is False

    def test_schema_drop_from_response(self):
        loader = SchemaLoader()
        schema = loader.load_schema("foobar")

        schema_by_name = {f["name"]: f for f in schema}

        assert schema_by_name["_id"]["drop_from_response"] is True
        assert schema_by_name["name"]["drop_from_response"] is False

    def test_schema_unique_fields(self):
        loader = SchemaLoader()
        schema = loader.load_schema("foobar")

        schema_by_name = {f["name"]: f for f in schema}

        assert schema_by_name["name"].get("unique") is True
        assert schema_by_name["email"].get("unique") is True

    def test_schema_allowed_values(self):
        loader = SchemaLoader()
        schema = loader.load_schema("foobar")

        schema_by_name = {f["name"]: f for f in schema}

        assert "allowed_values" in schema_by_name["status"]
        assert schema_by_name["status"]["allowed_values"] == ["active", "processing", "archived"]

    def test_nonexistent_schema(self):
        loader = SchemaLoader()
        with pytest.raises(FileNotFoundError, match="Schema for 'nonexistent' not found"):
            loader.load_schema("nonexistent")

    def test_load_schema_with_custom_dir(self, tmp_path):
        schema_data = [
            {
                "name": "test_field",
                "type": "str",
                "null": False,
                "post_value": True,
                "patch_value": True,
                "default": None
            }
        ]
        schema_file = tmp_path / "test_object.json"
        schema_file.write_text(json.dumps(schema_data))

        loader = SchemaLoader(schema_dir=str(tmp_path))
        schema = loader.load_schema("test_object")

        assert len(schema) == 1
        assert schema[0]["name"] == "test_field"
        assert schema[0]["type"] is str
