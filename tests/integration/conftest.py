"""
Integration test fixtures for real DynamoDB and SQS.

Expect DYNAMODB_ENDPOINT, SQS_HOSTS_CONFIG (and related env) to be set, e.g. by:
  INTEGRATION=1 source tests/envs.sh
If using setup-pytest.sh for containers, set DYNAMODB_ENDPOINT=http://localhost:58000
and SQS_HOSTS_CONFIG with endpoint http://localhost:54566 (see setup-pytest.sh).
"""

import os
import pytest
import boto3


# Table and queue used by integration tests (must exist or will be created where possible)
INTEGRATION_TABLE_NAME = "foobars"
INTEGRATION_QUEUE_KEY = "password_reset_requests"


def _dynamo_reachable():
    """Check if DynamoDB is reachable with current env."""
    endpoint = os.environ.get("DYNAMODB_ENDPOINT")
    if not endpoint:
        return False
    try:
        session = boto3.Session(
            aws_access_key_id=os.environ.get("DYNAMODB_ACCESS_KEY", "test"),
            aws_secret_access_key=os.environ.get("DYNAMODB_SECRET_KEY", "test"),
            region_name=os.environ.get("DYNAMODB_REGION", "us-east-1"),
        )
        client = session.client("dynamodb", endpoint_url=endpoint)
        client.list_tables()
        return True
    except Exception:
        return False


def _sqs_reachable():
    """Check if SQS is reachable (SQS_HOSTS_CONFIG set and at least one host connects)."""
    import json
    raw = os.environ.get("SQS_HOSTS_CONFIG", "[]")
    if not raw or raw == "[]":
        return False
    try:
        hosts = json.loads(raw)
        if not hosts:
            return False
        h = hosts[0]
        endpoint = h.get("SQS_ENDPOINT_URL") or h.get("endpoint_url")
        region = h.get("SQS_REGION_NAME") or h.get("region_name") or "us-east-1"
        # Resolve env var names (app config uses SQS_ACCESS_KEY_ID as key -> env var name)
        ak = h.get("SQS_ACCESS_KEY_ID", "test")
        if isinstance(ak, str) and ak and not ak.startswith("AK"):
            ak = os.environ.get(ak, "test")
        sk = h.get("SQS_SECRET_ACCESS_KEY", "test")
        if isinstance(sk, str) and sk and not sk.startswith("wJal"):
            sk = os.environ.get(sk, "test")
        session = boto3.Session(
            aws_access_key_id=ak or "test",
            aws_secret_access_key=sk or "test",
            region_name=region,
        )
        client = session.client("sqs", endpoint_url=endpoint, region_name=region)
        client.list_queues()
        return True
    except Exception:
        return False


def _require_integration_env():
    """Skip if integration env is not set (INTEGRATION=1 and services configured)."""
    if os.environ.get("INTEGRATION") != "1":
        pytest.skip(
            "Integration tests require INTEGRATION=1 and DynamoDB/SQS env (e.g. INTEGRATION=1 source tests/envs.sh)"
        )


def _ensure_sqs_queues():
    """Create SQS queues from SQS_HOSTS_CONFIG if they do not exist (e.g. LocalStack)."""
    import json
    raw = os.environ.get("SQS_HOSTS_CONFIG", "[]")
    if not raw or raw == "[]":
        return
    try:
        hosts = json.loads(raw)
        for h in hosts:
            endpoint = h.get("SQS_ENDPOINT_URL") or h.get("endpoint_url")
            region = h.get("SQS_REGION_NAME") or h.get("region_name") or "us-east-1"
            ak = h.get("SQS_ACCESS_KEY_ID", "test")
            if isinstance(ak, str) and ak and not ak.startswith("AK"):
                ak = os.environ.get(ak, "test")
            sk = h.get("SQS_SECRET_ACCESS_KEY", "test")
            if isinstance(sk, str) and sk and not sk.startswith("wJal"):
                sk = os.environ.get(sk, "test")
            session = boto3.Session(
                aws_access_key_id=ak or "test",
                aws_secret_access_key=sk or "test",
                region_name=region,
            )
            client = session.client("sqs", endpoint_url=endpoint, region_name=region)
            queues_config = h.get("queues", {})
            for qkey, qconf in queues_config.items():
                name = qconf.get("name", qkey) if isinstance(qconf, dict) else qconf
                try:
                    client.create_queue(QueueName=name)
                except Exception:
                    pass  # Queue may already exist
    except Exception:
        pass


@pytest.fixture(scope="module")
def dynamo_client():
    """Real DynamoDB resource; skip if unreachable."""
    _require_integration_env()
    if not _dynamo_reachable():
        pytest.skip("DynamoDB not reachable at DYNAMODB_ENDPOINT")
    session = boto3.Session(
        aws_access_key_id=os.environ.get("DYNAMODB_ACCESS_KEY", "test"),
        aws_secret_access_key=os.environ.get("DYNAMODB_SECRET_KEY", "test"),
        region_name=os.environ.get("DYNAMODB_REGION", "us-east-1"),
    )
    return session.resource(
        "dynamodb",
        endpoint_url=os.environ.get("DYNAMODB_ENDPOINT"),
    )


def _ensure_foobars_table(client):
    """Create foobars table if it does not exist."""
    dynamo_client_low = client.meta.client
    try:
        dynamo_client_low.describe_table(TableName=INTEGRATION_TABLE_NAME)
    except dynamo_client_low.exceptions.ResourceNotFoundException:
        dynamo_client_low.create_table(
            TableName=INTEGRATION_TABLE_NAME,
            AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        # Wait for table to be active
        waiter = dynamo_client_low.get_waiter("table_exists")
        waiter.wait(TableName=INTEGRATION_TABLE_NAME)


@pytest.fixture(scope="module")
def dynamo_repo(dynamo_client):
    """DynamoRepository for foobars table using real DynamoDB."""
    _ensure_foobars_table(dynamo_client)
    from app.repositories.dynamo_repository import DynamoRepository
    return DynamoRepository(
        table_name=INTEGRATION_TABLE_NAME,
        key_field="key",
        dynamo_client=dynamo_client,
    )


@pytest.fixture(scope="module")
def flask_app_integration():
    """Flask app with config from env (real SQS/DynamoDB). Skip if SQS unreachable."""
    _require_integration_env()
    if not _sqs_reachable():
        pytest.skip("SQS not reachable (SQS_HOSTS_CONFIG)")
    from flask import Flask
    import app.config as app_config
    from app.api_v1 import health
    from app.api_v2 import objects
    import app.helpers.error as aferror
    from app.services.sqs_factory import SQSFactory
    from app.repositories.repository_factory import RepositoryFactory

    flask_app = Flask(__name__, instance_relative_config=True)
    flask_app.config.from_object(app_config.DevelopmentConfig)
    flask_app.config["TESTING"] = True
    # Ensure env-based config is used
    flask_app.config["DYNAMODB_ENDPOINT"] = os.environ.get("DYNAMODB_ENDPOINT")
    flask_app.config["DYNAMODB_REGION"] = os.environ.get("DYNAMODB_REGION", "us-east-1")
    flask_app.config["DYNAMODB_ACCESS_KEY"] = os.environ.get("DYNAMODB_ACCESS_KEY", "test")
    flask_app.config["DYNAMODB_SECRET_KEY"] = os.environ.get("DYNAMODB_SECRET_KEY", "test")
    flask_app.config["SQS_HOSTS_CONFIG"] = os.environ.get("SQS_HOSTS_CONFIG", "[]")

    _ensure_sqs_queues()

    RepositoryFactory.configure(
        "dynamo",
        flask_app=flask_app,
    )
    RepositoryFactory._dynamo_client = None  # force use of get_dynamodb_client
    dynamo_client_real = RepositoryFactory.get_dynamodb_client(flask_app)
    RepositoryFactory.configure("dynamo", dynamo_client=dynamo_client_real, flask_app=flask_app)

    SQSFactory.configure(flask_app)
    SQSFactory.get_service().connect()

    flask_app.register_blueprint(health.bp)
    flask_app.register_blueprint(objects.bp)
    for code in [400, 401, 403, 404, 409, 500]:
        flask_app.register_error_handler(code, aferror.handle_error)

    yield flask_app

    try:
        SQSFactory.close()
    except Exception:
        pass


@pytest.fixture(scope="module")
def sqs_service(flask_app_integration):
    """Real SQS service (SQSService) from SQSFactory."""
    from app.services.sqs_factory import SQSFactory
    return SQSFactory.get_service()


@pytest.fixture
def unique_key():
    """Generate a unique key for integration test items to avoid collisions."""
    import uuid
    return f"it-{uuid.uuid4().hex[:12]}"
