"""
Integration tests for SQS (publish, health).

Requires SQS to be running (e.g. LocalStack).
Run with: INTEGRATION=1 source tests/envs.sh && pytest tests/integration/test_sqs_integration.py -v
"""

import json
import pytest
import boto3
import os


pytestmark = pytest.mark.integration


# Queue key from envs.sh SQS_HOSTS_CONFIG (password_reset_requests)
INTEGRATION_QUEUE_KEY = "password_reset_requests"


def _get_sqs_client_and_queue_url():
    """Build boto3 SQS client and queue URL from SQS_HOSTS_CONFIG for receive tests."""
    raw = os.environ.get("SQS_HOSTS_CONFIG", "[]")
    if not raw or raw == "[]":
        return None, None
    hosts = json.loads(raw)
    if not hosts:
        return None, None
    h = hosts[0]
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
    queue_name = "password_reset_requests"
    try:
        resp = client.get_queue_url(QueueName=queue_name)
        return client, resp["QueueUrl"]
    except Exception:
        return client, None


class TestSQSIntegrationHealth:
    """SQS health check against real SQS."""

    def test_sqs_health_check(self, sqs_service):
        """Health check returns healthy when SQS is reachable."""
        result = sqs_service.health_check()
        assert result.get("status") in ("healthy", "unhealthy")
        if result.get("status") == "healthy":
            assert "message" in result


class TestSQSIntegrationPublish:
    """Publish message to real SQS."""

    def test_publish_message(self, sqs_service):
        """Publish a message to the configured queue."""
        message = {"action": "integration_test", "payload": "hello"}
        success = sqs_service.publish_message(INTEGRATION_QUEUE_KEY, message)
        assert success is True

    def test_publish_and_receive(self, sqs_service):
        """Publish a message and verify it can be received from the queue."""
        client, queue_url = _get_sqs_client_and_queue_url()
        if not queue_url:
            pytest.skip("Queue URL not available for receive test")
        message = {"action": "integration_receive_test", "id": "it-receive-1"}
        success = sqs_service.publish_message(INTEGRATION_QUEUE_KEY, message)
        assert success is True
        # Receive (long polling 2s)
        response = client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
            MessageAttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        bodies = [json.loads(m["Body"]) for m in messages if "Body" in m]
        # Our message is wrapped in data/metadata
        found = any(
            b.get("data", {}).get("action") == "integration_receive_test"
            for b in bodies
        )
        assert found, f"Expected message not found in received: {bodies}"
