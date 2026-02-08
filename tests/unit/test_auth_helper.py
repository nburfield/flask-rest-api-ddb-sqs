"""Tests for app.helpers.auth_helper module."""

import time
import pytest
import jwt


class TestLoadAuth:

    def test_valid_jwt_sets_claimset(self, app, jwt_token):
        with app.test_request_context(
            '/api/v2/foobars',
            headers={'Authorization': f'Bearer {jwt_token}'}
        ):
            from flask import g
            from app.helpers.auth_helper import load_auth
            load_auth()

            assert g.claimset['user_name'] == 'testuser'
            assert g.jwt == jwt_token

    def test_no_auth_header(self, app):
        with app.test_request_context('/api/v2/foobars'):
            from flask import g
            from app.helpers.auth_helper import load_auth
            load_auth()

            assert g.claimset == {}
            assert g.jwt is None

    def test_invalid_jwt(self, app):
        with app.test_request_context(
            '/api/v2/foobars',
            headers={'Authorization': 'Bearer invalid.jwt.token'}
        ):
            from flask import g
            from app.helpers.auth_helper import load_auth
            load_auth()

            assert g.claimset == {}
            assert g.jwt is None

    def test_expired_jwt(self, app):
        payload = {
            'user_name': 'testuser',
            'exp': int(time.time()) - 3600,
            'iat': int(time.time()) - 7200,
        }
        # Use app's key so token is valid but expired (HS256 key must be 32+ bytes)
        key = app.config['JWT_SECRET_KEY']
        expired_token = jwt.encode(payload, key, algorithm='HS256')

        with app.test_request_context(
            '/api/v2/foobars',
            headers={'Authorization': f'Bearer {expired_token}'}
        ):
            from flask import g
            from app.helpers.auth_helper import load_auth
            load_auth()

            assert g.claimset == {}
            assert g.jwt is None

    def test_wrong_secret_key(self, app):
        payload = {
            'user_name': 'testuser',
            'exp': int(time.time()) + 3600,
        }
        # Different key, 32+ bytes to avoid PyJWT InsecureKeyLengthWarning
        wrong_key = 'wrong-jwt-secret-key-for-hs256-min-32-bytes!'
        bad_token = jwt.encode(payload, wrong_key, algorithm='HS256')

        with app.test_request_context(
            '/api/v2/foobars',
            headers={'Authorization': f'Bearer {bad_token}'}
        ):
            from flask import g
            from app.helpers.auth_helper import load_auth
            load_auth()

            assert g.claimset == {}
            assert g.jwt is None

    def test_bearer_prefix_stripped(self, app, jwt_token):
        with app.test_request_context(
            '/api/v2/foobars',
            headers={'Authorization': f'Bearer {jwt_token}'}
        ):
            from flask import g
            from app.helpers.auth_helper import load_auth
            load_auth()

            # g.jwt should not contain "Bearer"
            assert 'Bearer' not in g.jwt


class TestSetAuth:

    def test_cors_headers(self, client):
        response = client.get('/api/v1/health/flask')

        assert response.headers.get('Access-Control-Allow-Origin') == '*'
        assert 'Content-Type' in response.headers.get('Access-Control-Allow-Headers', '')
        assert 'Authorization' in response.headers.get('Access-Control-Allow-Headers', '')
        assert 'GET' in response.headers.get('Access-Control-Allow-Methods', '')
        assert 'POST' in response.headers.get('Access-Control-Allow-Methods', '')
        assert 'PATCH' in response.headers.get('Access-Control-Allow-Methods', '')
        assert 'DELETE' in response.headers.get('Access-Control-Allow-Methods', '')
        assert response.headers.get('Access-Control-Allow-Credentials') == 'true'


class TestRequireAuth:

    def test_decorated_endpoint_with_valid_auth(self, app, jwt_token):
        from app.helpers.auth_helper import require_auth, load_auth

        app.before_request(load_auth)

        @app.route('/test-auth')
        @require_auth
        def protected():
            return 'ok'

        with app.test_client() as test_client:
            response = test_client.get(
                '/test-auth',
                headers={'Authorization': f'Bearer {jwt_token}'}
            )
            assert response.status_code == 200

    def test_decorated_endpoint_without_auth(self, app):
        from app.helpers.auth_helper import require_auth, load_auth

        app.before_request(load_auth)

        @app.route('/test-auth-none')
        @require_auth
        def protected_none():
            return 'ok'

        with app.test_client() as test_client:
            response = test_client.get('/test-auth-none')
            assert response.status_code == 401
