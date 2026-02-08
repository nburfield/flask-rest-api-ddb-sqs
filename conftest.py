"""
Root conftest.py for the Flask API test suite.

Provides fixtures for Flask app, test client, mock repositories,
JWT tokens, and factory resets.
"""

import os
import json
import subprocess
import time
import pytest
import jwt

# Load tests/envs.sh so INTEGRATION and other vars from that file apply (no need to source in shell)
# Use env -0 (null-terminated) so multi-line values (e.g. SQS_HOSTS_CONFIG) parse correctly
_CONFTEST_DIR = os.path.dirname(os.path.abspath(__file__))
_ENVS_SH = os.path.join(_CONFTEST_DIR, "tests", "envs.sh")
if os.path.isfile(_ENVS_SH):
    try:
        _out = subprocess.run(
            ["bash", "-c", f"set -a && . '{_ENVS_SH}' && set +a && env -0"],
            capture_output=True,
            text=True,
            cwd=_CONFTEST_DIR,
        )
        if _out.returncode == 0 and _out.stdout:
            for _chunk in _out.stdout.split("\0"):
                if "=" in _chunk:
                    _k, _, _v = _chunk.partition("=")
                    os.environ[_k] = _v
    except Exception:
        pass

# Debug: uncomment to verify envs.sh was loaded when running pytest
# import sys; sys.stderr.write(f"conftest: INTEGRATION={os.environ.get('INTEGRATION')!r}\n")

# Test keys: HS256 requires at least 32 bytes (RFC 7518) to avoid PyJWT warnings
TEST_SECRET_KEY = 'test-secret-key-for-flask-min-32-bytes-long!'
TEST_JWT_SECRET_KEY = 'test-jwt-secret-key-for-hs256-min-32-bytes!'

# Set test environment before importing the app
# When INTEGRATION=1 (set in tests/envs.sh), leave DYNAMODB_* and SQS_HOSTS_CONFIG from env
os.environ['APP_SETTINGS'] = 'app.config.DevelopmentConfig'
os.environ['FLASK_DEBUG'] = 'true'
os.environ['SECRET_KEY'] = os.environ.get('SECRET_KEY', TEST_SECRET_KEY)
os.environ['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', TEST_JWT_SECRET_KEY)
if os.environ.get('INTEGRATION') != '1':
    os.environ.setdefault('DYNAMODB_ENDPOINT', 'http://localhost:98000')
    os.environ.setdefault('DYNAMODB_REGION', 'us-east-1')
    os.environ.setdefault('DYNAMODB_ACCESS_KEY', 'fakeMyKeyId')
    os.environ.setdefault('DYNAMODB_SECRET_KEY', 'fakeSecretAccessKey')
    os.environ['S3_HOSTS_CONFIG'] = '[]'
    os.environ['SQS_HOSTS_CONFIG'] = '[]'


from app.repositories.repository_factory import RepositoryFactory
from app.services.s3_factory import S3Factory
from app.services.sqs_factory import SQSFactory


def _reset_all_factories():
    """Reset all singleton factories to clean state."""
    RepositoryFactory._instances = {}
    RepositoryFactory._backend = None
    RepositoryFactory._mongo_client = None
    RepositoryFactory._dynamo_client = None
    S3Factory._instance = None
    S3Factory._configured = False
    SQSFactory._instance = None
    SQSFactory._configured = False


class ConditionalCheckFailedException(Exception):
    pass


class ResourceNotFoundException(Exception):
    pass


class MockDynamoTable:
    """In-memory mock of a DynamoDB table for testing."""

    class Meta:
        class Client:
            class _Exceptions:
                ConditionalCheckFailedException = ConditionalCheckFailedException
                ResourceNotFoundException = ResourceNotFoundException

            exceptions = _Exceptions()

        client = Client()

    meta = Meta()

    def __init__(self, table_name):
        self.table_name = table_name
        self.items = {}

    def put_item(self, Item):
        key = Item.get('key')
        self.items[key] = dict(Item)

    def get_item(self, Key):
        key = Key.get('key')
        item = self.items.get(key)
        if item:
            return {'Item': dict(item)}
        return {}

    def update_item(self, **kwargs):
        key_val = kwargs['Key'].get('key')
        if key_val not in self.items:
            return

        # Check condition expression for version
        if 'ConditionExpression' in kwargs:
            expr_vals = kwargs.get('ExpressionAttributeValues', {})
            expected = expr_vals.get(':expected_version')
            if expected is not None:
                current = self.items[key_val].get('version', 0)
                if current != expected:
                    raise ConditionalCheckFailedException("Version mismatch")

        # Apply updates from SET expression
        update_expr = kwargs.get('UpdateExpression', '')
        expr_vals = kwargs.get('ExpressionAttributeValues', {})
        expr_names = kwargs.get('ExpressionAttributeNames', {})

        if update_expr.startswith('SET '):
            assignments = update_expr[4:].split(', ')
            for assignment in assignments:
                parts = assignment.split(' = ')
                if len(parts) == 2:
                    attr_placeholder, val_placeholder = parts[0].strip(), parts[1].strip()
                    # Resolve attribute name
                    attr_name = expr_names.get(attr_placeholder, attr_placeholder.lstrip('#'))
                    if attr_name.startswith('attr_'):
                        attr_name = attr_name[5:]
                    # Resolve value
                    value = expr_vals.get(val_placeholder)
                    if value is not None:
                        self.items[key_val][attr_name] = value

    def delete_item(self, Key):
        key = Key.get('key')
        self.items.pop(key, None)

    def scan(self, **kwargs):
        items = list(self.items.values())

        # Apply filter expression
        filter_expr = kwargs.get('FilterExpression', '')
        expr_vals = kwargs.get('ExpressionAttributeValues', {})
        expr_names = kwargs.get('ExpressionAttributeNames', {})

        if filter_expr:
            filtered = []
            for item in items:
                if self._matches_filter(item, filter_expr, expr_vals, expr_names):
                    filtered.append(item)
            items = filtered

        return {'Items': [dict(item) for item in items]}

    def _matches_filter(self, item, filter_expr, expr_vals, expr_names):
        """Simple filter expression evaluator for testing."""
        # Split on AND
        conditions = filter_expr.split(' AND ')
        for condition in conditions:
            condition = condition.strip()

            # Handle equality: #field = :value
            if ' = ' in condition and ' <> ' not in condition and 'IN' not in condition:
                parts = condition.split(' = ')
                if len(parts) == 2:
                    field_placeholder = parts[0].strip()
                    val_placeholder = parts[1].strip()
                    field_name = expr_names.get(field_placeholder, field_placeholder)
                    expected_val = expr_vals.get(val_placeholder)
                    actual_val = item.get(field_name)
                    if actual_val != expected_val:
                        return False

            # Handle not-equal: #field <> :value
            elif ' <> ' in condition:
                parts = condition.split(' <> ')
                if len(parts) == 2:
                    field_placeholder = parts[0].strip()
                    val_placeholder = parts[1].strip()
                    field_name = expr_names.get(field_placeholder, field_placeholder)
                    expected_val = expr_vals.get(val_placeholder)
                    actual_val = item.get(field_name)
                    if actual_val == expected_val:
                        return False

        return True


class MockDynamoClient:
    """Mock DynamoDB client that returns MockDynamoTable instances."""

    def __init__(self):
        self.tables = {}

    def Table(self, table_name):
        if table_name not in self.tables:
            self.tables[table_name] = MockDynamoTable(table_name)
        return self.tables[table_name]


class MockSQSService:
    """Mock SQS service for testing."""

    def __init__(self):
        self.published_messages = []
        self.connections = {}

    def connect(self):
        pass

    def publish_message(self, queue_key, message, delay_seconds=0, host_name=None):
        self.published_messages.append({
            'queue_key': queue_key,
            'message': message,
            'delay_seconds': delay_seconds,
            'host_name': host_name
        })
        return True

    def get_available_hosts(self):
        return list(self.connections.keys())

    def health_check(self):
        return {'status': 'healthy'}

    def close(self):
        pass


class MockS3Service:
    """Mock S3 service for testing."""

    def __init__(self):
        self.connections = {}

    def get_signed_url(self, *args, **kwargs):
        return "https://mock-s3-url.com/signed"

    def get_signed_put_url(self, *args, **kwargs):
        return "https://mock-s3-url.com/signed-put"

    def copy_object(self, *args, **kwargs):
        return True

    def object_exists(self, *args, **kwargs):
        return False

    def get_object_metadata(self, *args, **kwargs):
        return {'ContentLength': 0}

    def list_objects(self, *args, **kwargs):
        return []

    def get_available_hosts(self):
        return list(self.connections.keys())

    def get_host_buckets(self, host_name):
        return {}

    def health_check(self):
        return {'status': 'healthy'}

    def close(self):
        pass


@pytest.fixture
def mock_dynamo_client():
    """Provide a mock DynamoDB client."""
    return MockDynamoClient()


@pytest.fixture
def mock_sqs_service():
    """Provide a mock SQS service."""
    return MockSQSService()


@pytest.fixture
def mock_s3_service():
    """Provide a mock S3 service."""
    return MockS3Service()


@pytest.fixture
def app(mock_dynamo_client, mock_sqs_service, mock_s3_service):
    """Create a Flask application for testing with mocked services."""
    from flask import Flask
    import app.helpers.error as aferror
    from app.api_v1 import health
    from app.api_v2 import objects

    # Reset all factories to ensure clean state
    _reset_all_factories()

    flask_app = Flask(__name__, instance_relative_config=True)
    flask_app.config['TESTING'] = True
    flask_app.config['SECRET_KEY'] = TEST_SECRET_KEY
    flask_app.config['JWT_SECRET_KEY'] = TEST_JWT_SECRET_KEY
    flask_app.config['DYNAMODB_ENDPOINT'] = 'http://localhost:98000'
    flask_app.config['DYNAMODB_REGION'] = 'us-east-1'
    flask_app.config['DYNAMODB_ACCESS_KEY'] = 'fakeMyKeyId'
    flask_app.config['DYNAMODB_SECRET_KEY'] = 'fakeSecretAccessKey'
    flask_app.config['S3_HOSTS_CONFIG'] = '[]'
    flask_app.config['SQS_HOSTS_CONFIG'] = '[]'

    # Configure factories with mocks
    RepositoryFactory.configure(
        "dynamo",
        dynamo_client=mock_dynamo_client,
        flask_app=flask_app
    )

    S3Factory._instance = mock_s3_service
    S3Factory._configured = True

    SQSFactory._instance = mock_sqs_service
    SQSFactory._configured = True

    # Register blueprints
    flask_app.register_blueprint(health.bp)
    flask_app.register_blueprint(objects.bp)

    # Register error handlers
    for code in [400, 401, 403, 404, 409, 500]:
        flask_app.register_error_handler(code, aferror.handle_error)

    yield flask_app

    # Cleanup after test
    _reset_all_factories()


@pytest.fixture
def client(app):
    """Create a Flask test client."""
    return app.test_client()


@pytest.fixture
def jwt_token():
    """Generate a valid JWT token for authenticated requests."""
    payload = {
        'user_name': 'testuser',
        'roles': ['sec:globaladmin'],
        'exp': int(time.time()) + 3600,
        'iat': int(time.time()),
    }
    token = jwt.encode(payload, TEST_JWT_SECRET_KEY, algorithm='HS256')
    return token


@pytest.fixture
def auth_headers(jwt_token):
    """Provide authorization headers with a valid JWT."""
    return {
        'Authorization': f'Bearer {jwt_token}',
        'Content-Type': 'application/json'
    }


@pytest.fixture
def json_headers():
    """Provide JSON content-type headers without auth."""
    return {
        'Content-Type': 'application/json'
    }


@pytest.fixture
def sample_foobar_data():
    """Provide sample foobar data for POST requests."""
    return {
        'name': 'Test Foobar',
        'email': 'test@example.com',
        'phone': '+12025551234',
        'status': 'active'
    }


@pytest.fixture
def sample_foobar_item():
    """Provide a complete foobar item as stored in the database."""
    return {
        'key': 'abc123def456',
        'name': 'Test Foobar',
        'email': 'test@example.com',
        'phone': '+12025551234',
        'status': 'active',
        'created_user': 'testuser',
        'created_dt': 1700000000.0,
        'updated_user': 'testuser',
        'updated_dt': 1700000000.0,
        'version': 0,
        'object_type': 'foobar'
    }
