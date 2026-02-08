"""Tests for app.repositories.repository_factory module."""

import pytest
from unittest.mock import MagicMock
from app.repositories.repository_factory import RepositoryFactory
from conftest import _reset_all_factories


@pytest.fixture(autouse=True)
def clean_factory():
    """Reset RepositoryFactory before and after each test in this module."""
    _reset_all_factories()
    yield
    _reset_all_factories()


class TestRepositoryFactoryConfigure:

    def test_configure_dynamo(self):
        mock_client = MagicMock()
        RepositoryFactory.configure("dynamo", dynamo_client=mock_client)

        assert RepositoryFactory._backend == "dynamo"
        assert RepositoryFactory._dynamo_client == mock_client

    def test_configure_case_insensitive(self):
        mock_client = MagicMock()
        RepositoryFactory.configure("DYNAMO", dynamo_client=mock_client)

        assert RepositoryFactory._backend == "dynamo"


class TestRepositoryFactoryGet:

    def test_get_dynamo_repository(self):
        mock_client = MagicMock()
        RepositoryFactory.configure("dynamo", dynamo_client=mock_client)

        repo = RepositoryFactory.get("test_table")
        assert repo is not None

    def test_get_returns_cached_instance(self):
        mock_client = MagicMock()
        RepositoryFactory.configure("dynamo", dynamo_client=mock_client)

        repo1 = RepositoryFactory.get("test_table")
        repo2 = RepositoryFactory.get("test_table")

        assert repo1 is repo2

    def test_get_different_tables(self):
        mock_client = MagicMock()
        RepositoryFactory.configure("dynamo", dynamo_client=mock_client)

        repo1 = RepositoryFactory.get("table1")
        repo2 = RepositoryFactory.get("table2")

        assert repo1 is not repo2

    def test_get_not_configured(self):
        with pytest.raises(ValueError, match="RepositoryFactory not configured"):
            RepositoryFactory.get("test_table")

    def test_get_unsupported_backend(self):
        RepositoryFactory.configure("unsupported")

        with pytest.raises(ValueError, match="Unsupported backend"):
            RepositoryFactory.get("test_table")

    def test_get_custom_key_field(self):
        mock_client = MagicMock()
        RepositoryFactory.configure("dynamo", dynamo_client=mock_client)

        repo = RepositoryFactory.get("custom_key_test", key_field="custom_id")
        assert repo.key_field == "custom_id"

    def test_get_custom_table_name(self):
        mock_client = MagicMock()
        RepositoryFactory.configure("dynamo", dynamo_client=mock_client)

        repo = RepositoryFactory.get("object_type", table_name="custom_table")
        mock_client.Table.assert_called_with("custom_table")
