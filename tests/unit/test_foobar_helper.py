"""Tests for app.helpers.foobar module."""

import pytest
from unittest.mock import MagicMock, patch
from app.helpers.foobar import FoobarHelper


class TestFoobarHelperInit:

    def test_initializes_with_foobar_model(self, app):
        helper = FoobarHelper()
        assert helper.model.object_type == "foobar"
        assert helper.model.database_name == "foobars"


class TestFoobarHelperUpdate:

    def test_update_without_status_change(self, app):
        with app.test_request_context('/api/v2/foobars/abc'):
            helper = FoobarHelper()
            helper.model = MagicMock()
            helper.model.schema_by_name = {
                "updated_dt": {"default": lambda: 200.0}
            }
            helper.model.get.return_value = {
                "key": "abc",
                "status": "active",
                "version": 0
            }
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

    def test_update_nonexistent_delegates_to_parent(self, app):
        with app.test_request_context('/api/v2/foobars/nonexistent'):
            helper = FoobarHelper()
            helper.model = MagicMock()
            helper.model.schema_by_name = {
                "updated_dt": {"default": lambda: 200.0}
            }
            helper.model.get.return_value = None

            with pytest.raises(Exception):
                helper.update(
                    "nonexistent",
                    {"email": "new@example.com", "version": 0},
                    "testuser"
                )


class TestPublishFoobarRequest:

    def test_publish_success(self, app, mock_sqs_service):
        with app.app_context():
            helper = FoobarHelper()
            foobar_data = {"name": "Test", "key": "abc"}

            helper.publish_foobar_request(foobar_data)

            assert len(mock_sqs_service.published_messages) == 1
            assert mock_sqs_service.published_messages[0]['queue_key'] == 'foobar_requests'
            assert mock_sqs_service.published_messages[0]['message'] == foobar_data

    def test_publish_handles_exception(self, app):
        with app.app_context():
            helper = FoobarHelper()

            with patch('app.helpers.foobar.SQSFactory') as mock_factory:
                mock_service = MagicMock()
                mock_service.publish_message.side_effect = Exception("Connection failed")
                mock_factory.get_service.return_value = mock_service

                # Should not raise
                helper.publish_foobar_request({"name": "Test"})
