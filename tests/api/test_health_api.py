"""Tests for app.api_v1.health endpoints."""

import pytest


class TestHealthFlask:

    def test_get_health_flask(self, client):
        response = client.get('/api/v1/health/flask')
        assert response.status_code == 200

        data = response.get_json()
        assert data['status'] == 'success'
        assert data['message'] == 'Flask is running'

    def test_health_flask_cors_headers(self, client):
        response = client.get('/api/v1/health/flask')

        assert response.headers.get('Access-Control-Allow-Origin') == '*'
        assert 'Content-Type' in response.headers.get('Access-Control-Allow-Headers', '')

    def test_health_flask_json_content_type(self, client):
        response = client.get('/api/v1/health/flask')
        assert 'application/json' in response.content_type


class TestHealthUserdata:

    def test_get_health_userdata(self, client):
        response = client.get('/api/v1/health/userdata')
        assert response.status_code == 200

        data = response.get_json()
        assert data['status'] == 'success'

    def test_userdata_includes_headers(self, client):
        response = client.get(
            '/api/v1/health/userdata',
            headers={'X-Custom-Header': 'test-value'}
        )
        data = response.get_json()
        assert data['status'] == 'success'
        # Headers should be in the message (excluding Authorization and Accept)
        assert 'X-Custom-Header' in data['message']

    def test_userdata_excludes_authorization(self, client, jwt_token):
        response = client.get(
            '/api/v1/health/userdata',
            headers={'Authorization': f'Bearer {jwt_token}'}
        )
        data = response.get_json()
        # Authorization should be excluded from the message
        assert 'Authorization' not in data['message']
