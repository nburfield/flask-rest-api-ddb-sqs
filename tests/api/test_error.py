"""Tests for app.helpers.error module."""

import pytest
from werkzeug.exceptions import NotFound, BadRequest, Unauthorized, Forbidden, Conflict


class TestHandleError:

    def test_400_error(self, client):
        # Trigger a 400 by posting invalid JSON
        response = client.post(
            '/api/v2/foobars',
            data='not-json',
            content_type='text/plain'
        )
        assert response.status_code == 400
        data = response.get_json()
        assert data['status'] == 400

    def test_404_error(self, client):
        response = client.get('/api/v2/foobars/nonexistent-key')
        assert response.status_code in [404, 500]

    def test_404_unsupported_object_type(self, client):
        response = client.get('/api/v2/unknown_objects')
        assert response.status_code == 404
        data = response.get_json()
        assert data['status'] == 404

    def test_error_response_structure(self, client):
        response = client.get('/api/v2/unknown_objects')
        data = response.get_json()

        assert 'status' in data
        assert 'message' in data
