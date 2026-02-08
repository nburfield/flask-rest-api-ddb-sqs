"""
Integration tests for DynamoDB via DynamoRepository.

Requires DynamoDB to be running (e.g. DynamoDB Local or LocalStack).
Run with: INTEGRATION=1 source tests/envs.sh && pytest tests/integration/test_dynamodb_integration.py -v
"""

import pytest


pytestmark = pytest.mark.integration


class TestDynamoDBIntegrationCRUD:
    """CRUD operations against real DynamoDB."""

    def test_create_and_get(self, dynamo_repo, unique_key):
        """Create an item and retrieve it."""
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "Integration Test",
            "email": f"it-{unique_key}@example.com",
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        got = dynamo_repo.get(unique_key)
        assert got is not None
        assert got["key"] == unique_key
        assert got["name"] == "Integration Test"
        assert got["object_type"] == "foobar"

    def test_update(self, dynamo_repo, unique_key):
        """Create, update, and verify."""
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "Original",
            "email": f"up-{unique_key}@example.com",
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        updated = dynamo_repo.update(unique_key, {"name": "Updated", "status": "processing"})
        assert updated is not None
        assert updated["name"] == "Updated"
        assert updated["status"] == "processing"

    def test_get_nonexistent(self, dynamo_repo, unique_key):
        """Get returns None for missing key."""
        got = dynamo_repo.get(unique_key)
        assert got is None

    def test_delete(self, dynamo_repo, unique_key):
        """Create, delete, then get returns None."""
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "To Delete",
            "email": f"del-{unique_key}@example.com",
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        result = dynamo_repo.delete(unique_key)
        assert result is True
        got = dynamo_repo.get(unique_key)
        assert got is None


class TestDynamoDBIntegrationList:
    """List operations against real DynamoDB."""

    def test_list_all_empty_or_contains_created(self, dynamo_repo, unique_key):
        """List foobar items; after creating one, list includes it."""
        items_before = dynamo_repo.list_all("foobar")
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "List Test",
            "email": f"list-{unique_key}@example.com",
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        items_after = dynamo_repo.list_all("foobar")
        keys = [i["key"] for i in items_after]
        assert unique_key in keys

    def test_list_all_paginated(self, dynamo_repo, unique_key):
        """Paginated list returns (items, total_count)."""
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "Paginated",
            "email": f"page-{unique_key}@example.com",
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        results, total = dynamo_repo.list_all_paginated("foobar", start=0, limit=10)
        assert isinstance(results, list)
        assert isinstance(total, int)
        assert total >= 1
        keys = [r["key"] for r in results]
        assert unique_key in keys or total > 10

    def test_list_all_with_filter(self, dynamo_repo, unique_key):
        """List with status filter."""
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "Filter Test",
            "email": f"filter-{unique_key}@example.com",
            "status": "archived",
            "version": 0,
        }
        dynamo_repo.create(item)
        filters = {"status": {"operator": "eq", "values": ["archived"]}}
        items = dynamo_repo.list_all("foobar", filters=filters)
        keys = [i["key"] for i in items]
        assert unique_key in keys
        for i in items:
            assert i.get("status") == "archived"


class TestDynamoDBIntegrationGetByField:
    """get_by_field against real DynamoDB."""

    def test_get_by_field(self, dynamo_repo, unique_key):
        """Create item and retrieve by email."""
        email = f"byfield-{unique_key}@example.com"
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "By Field",
            "email": email,
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        found = dynamo_repo.get_by_field("email", email)
        assert found is not None
        assert found["email"] == email
        assert found["key"] == unique_key

    def test_get_by_field_not_found(self, dynamo_repo, unique_key):
        """get_by_field returns None when no match."""
        found = dynamo_repo.get_by_field("email", f"nonexistent-{unique_key}@example.com")
        assert found is None


class TestDynamoDBIntegrationUniqueCheck:
    """check_unique_values against real DynamoDB."""

    def test_check_unique_no_conflict(self, dynamo_repo, unique_key):
        """check_unique_values returns [] when value is unique."""
        email = f"unique-{unique_key}@example.com"
        conflicts = dynamo_repo.check_unique_values("foobar", {"email": email})
        assert conflicts == []

    def test_check_unique_conflict(self, dynamo_repo, unique_key):
        """check_unique_values returns field name when value exists."""
        email = f"conflict-{unique_key}@example.com"
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "Conflict",
            "email": email,
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        conflicts = dynamo_repo.check_unique_values("foobar", {"email": email})
        assert "email" in conflicts

    def test_check_unique_exclude_key(self, dynamo_repo, unique_key):
        """check_unique_values with exclude_key ignores that key (for updates)."""
        email = f"exclude-{unique_key}@example.com"
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "Exclude",
            "email": email,
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        conflicts = dynamo_repo.check_unique_values("foobar", {"email": email}, exclude_key=unique_key)
        assert "email" not in conflicts


class TestDynamoDBIntegrationUpdateByVersion:
    """update_by_version (optimistic locking) against real DynamoDB."""

    def test_update_by_version_success(self, dynamo_repo, unique_key):
        """update_by_version succeeds when version matches."""
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "Versioned",
            "email": f"ver-{unique_key}@example.com",
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        updated = dynamo_repo.update_by_version(
            unique_key,
            {"name": "Versioned Updated", "version": 1},
            expected_version=0,
        )
        assert updated is not None
        assert updated["name"] == "Versioned Updated"
        assert updated.get("version") == 1

    def test_update_by_version_conflict(self, dynamo_repo, unique_key):
        """update_by_version returns None when version does not match."""
        item = {
            "key": unique_key,
            "object_type": "foobar",
            "name": "Conflict Ver",
            "email": f"verconf-{unique_key}@example.com",
            "status": "active",
            "version": 0,
        }
        dynamo_repo.create(item)
        updated = dynamo_repo.update_by_version(
            unique_key,
            {"name": "Wrong", "version": 1},
            expected_version=99,
        )
        assert updated is None
