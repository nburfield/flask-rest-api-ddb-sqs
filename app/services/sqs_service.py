"""
SQS Service for handling message queue operations with multi-provider support
"""

import json
import logging
import threading
from typing import Dict, Any, Optional, List
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config


class SQSConnection:
    """
    Individual SQS connection wrapper supporting multiple providers
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.client = None
        self._connection_lock = threading.Lock()
        self._reconnect_delay = 1
        self._max_reconnect_delay = 30
        self._logger = logging.getLogger(f"{__name__}.{name}")

        # Provider-specific configurations
        self.provider = config.get('provider', 'aws').lower()
        self.endpoint_url = config.get('endpoint_url')
        self.region_name = config.get('region_name', 'us-east-1')
        self.use_ssl = config.get('use_ssl', True)
        self.verify_ssl = config.get('verify_ssl', True)

        # Connection parameters
        self.access_key_id = config.get('access_key_id')
        self.secret_access_key = config.get('secret_access_key')

        # Check if we should use IAM role (no explicit credentials)
        self._use_iam_role = self._should_use_iam_role()

        # Client configuration
        self.max_attempts = config.get('max_attempts', 3)
        self.connect_timeout = config.get('connect_timeout', 60)
        self.read_timeout = config.get('read_timeout', 60)
        self.retry_mode = config.get('retry_mode', 'standard')

        # Queue configurations (queue_key -> queue_url mapping)
        self.queues = config.get('queues', {})
        # Cache for queue URLs (queue_key -> queue_url)
        self._queue_url_cache: Dict[str, str] = {}

    def _should_use_iam_role(self) -> bool:
        """
        Determine if IAM role should be used instead of explicit credentials.
        This happens when credentials are None, empty, or placeholder values.

        Returns:
            bool: True if IAM role should be used, False otherwise
        """
        # List of placeholder values that indicate IAM role should be used
        placeholder_values = [
            'iam_role',
            'iam',
            'role',
            '',
            None
        ]

        access_key = (self.access_key_id or '').strip().lower()
        secret_key = (self.secret_access_key or '').strip().lower()

        # Use IAM role if both credentials are placeholders/empty
        if not access_key or not secret_key:
            return True

        if access_key in placeholder_values or secret_key in placeholder_values:
            return True

        return False

    def connect(self) -> bool:
        """
        Establish connection to SQS with automatic retry logic

        Returns:
            bool: True if connection successful, False otherwise
        """
        with self._connection_lock:
            try:
                if self.client:
                    return True

                self._logger.info("Connecting to SQS provider '%s' on host '%s'...", self.provider, self.name)

                # Configure client based on provider
                client_config = Config(
                    region_name=self.region_name,
                    retries={'max_attempts': self.max_attempts},
                    connect_timeout=self.connect_timeout,
                    read_timeout=self.read_timeout,
                    parameter_validation=False
                )

                # Create session - use IAM role if credentials are placeholders/empty
                if self._use_iam_role:
                    # No explicit credentials - boto3 will use IAM role automatically in Lambda
                    self._logger.info("Using IAM role for authentication (no explicit credentials provided)")
                    session = boto3.Session()
                else:
                    # Use explicit credentials
                    session = boto3.Session(
                        aws_access_key_id=self.access_key_id,
                        aws_secret_access_key=self.secret_access_key
                    )

                # Create client based on provider
                if self.provider == 'aws':
                    self.client = session.client('sqs', config=client_config)
                elif self.provider in ['localstack', 'custom']:
                    if not self.endpoint_url:
                        raise ValueError(f"endpoint_url required for provider '{self.provider}'")
                    self.client = session.client(
                        'sqs',
                        endpoint_url=self.endpoint_url,
                        config=client_config,
                        use_ssl=self.use_ssl,
                        verify=self.verify_ssl
                    )
                else:
                    raise ValueError(f"Unsupported SQS provider: {self.provider}")

                # Reset reconnect delay on successful connection
                self._reconnect_delay = 1
                self._logger.info("Successfully connected to SQS provider '%s' on host '%s'", self.provider, self.name)
                return True

            except Exception as e:
                self._logger.error("Failed to connect to SQS provider '%s' on host '%s': %s", self.provider, self.name, str(e))
                self._schedule_reconnect()
                return False

    def _schedule_reconnect(self):
        """Schedule a reconnection attempt with exponential backoff"""
        if self._reconnect_delay < self._max_reconnect_delay:
            self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

        self._logger.info("Scheduling reconnection attempt for '%s' in %s seconds", self.name, self._reconnect_delay)
        threading.Timer(self._reconnect_delay, self._reconnect).start()

    def _reconnect(self):
        """Attempt to reconnect to SQS"""
        if not self.connect():
            self._schedule_reconnect()

    def _ensure_connection(self) -> bool:
        """
        Ensure connection is established and healthy

        Returns:
            bool: True if connection is healthy, False otherwise
        """
        try:
            if not self.client:
                return self.connect()

            # Test connection with a simple operation
            self.client.list_queues()
            return True

        except Exception as e:
            self._logger.error("Connection health check failed for '%s': %s", self.name, str(e))
            return self.connect()

    def setup_queues(self):
        """
        Setup queues for this connection based on host-specific configuration
        """
        if not self._ensure_connection():
            self._logger.error("Cannot setup queues for '%s': no connection", self.name)
            return False

        try:
            # Create/ensure queues exist
            for queue_key, queue_config in self.queues.items():
                queue_name = queue_config.get('name') or queue_key

                # Skip if queue_name is already a full URL
                if queue_name.startswith('http://') or queue_name.startswith('https://'):
                    self._logger.info(
                        "Queue '%s' (key '%s') appears to be a full URL, skipping creation on host '%s'",
                        queue_name, queue_key, self.name
                    )
                    # Still cache the URL
                    self._queue_url_cache[queue_key] = queue_name
                    continue

                try:
                    # Check if queue already exists
                    try:
                        response = self.client.get_queue_url(QueueName=queue_name)
                        queue_url = response['QueueUrl']
                        self._logger.info(
                            "Queue '%s' (key '%s') already exists on host '%s'",
                            queue_name, queue_key, self.name
                        )
                    except ClientError as e:
                        error_code = e.response.get('Error', {}).get('Code', '')
                        if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
                            # Queue doesn't exist, create it
                            create_params = {'QueueName': queue_name}

                            # Add optional queue attributes if provided
                            attributes = queue_config.get('attributes', {})
                            if attributes:
                                create_params['Attributes'] = attributes

                            response = self.client.create_queue(**create_params)
                            queue_url = response['QueueUrl']
                            self._logger.info(
                                "Created queue '%s' (key '%s') on host '%s'",
                                queue_name, queue_key, self.name
                            )
                        else:
                            raise

                    # Cache the URL
                    self._queue_url_cache[queue_key] = queue_url

                except ClientError as e:
                    self._logger.error(
                        "Failed to setup queue '%s' (key '%s') on host '%s': %s",
                        queue_name, queue_key, self.name, str(e)
                    )
                except Exception as e:
                    self._logger.error(
                        "Failed to setup queue '%s' (key '%s') on host '%s': %s",
                        queue_name, queue_key, self.name, str(e)
                    )

            self._logger.info("Successfully setup queues for '%s'", self.name)
            return True

        except Exception as e:
            self._logger.error("Failed to setup queues for '%s': %s", self.name, str(e))
            return False

    def _get_queue_url(self, queue_key: str) -> Optional[str]:
        """
        Get queue URL for a given queue key, with caching

        Args:
            queue_key: Key of the queue configuration

        Returns:
            str: Queue URL or None if not found
        """
        # Check cache first
        if queue_key in self._queue_url_cache:
            return self._queue_url_cache[queue_key]

        if queue_key not in self.queues:
            self._logger.error("Queue key '%s' not found in configuration for host '%s'", queue_key, self.name)
            return None

        queue_config = self.queues[queue_key]
        queue_name = queue_config.get('name') or queue_key

        try:
            # Try to get queue URL directly if it's a full URL
            if queue_name.startswith('http://') or queue_name.startswith('https://'):
                queue_url = queue_name
            else:
                # Get queue URL by name
                response = self.client.get_queue_url(QueueName=queue_name)
                queue_url = response['QueueUrl']

            # Cache the URL
            self._queue_url_cache[queue_key] = queue_url
            return queue_url

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
                self._logger.error("Queue '%s' does not exist for host '%s'", queue_name, self.name)
            else:
                self._logger.error("Failed to get queue URL for '%s' on host '%s': %s", queue_name, self.name, str(e))
            return None
        except Exception as e:
            self._logger.error("Failed to get queue URL for '%s' on host '%s': %s", queue_name, self.name, str(e))
            return None

    def publish_message(self, queue_key: str, message: Dict[str, Any], delay_seconds: int = 0) -> bool:
        """
        Publish a message to a specific SQS queue

        Args:
            queue_key: Key of the queue configuration to publish to
            message: Message data to publish
            delay_seconds: Optional delay in seconds before message becomes available

        Returns:
            bool: True if message published successfully, False otherwise
        """
        if not self._ensure_connection():
            self._logger.error("Cannot publish message to '%s': no connection", self.name)
            return False

        queue_url = self._get_queue_url(queue_key)
        if not queue_url:
            self._logger.error("Cannot publish message: queue '%s' not found on host '%s'", queue_key, self.name)
            return False

        try:
            # Add metadata to message
            message_with_metadata = {
                'data': message,
                'metadata': {
                    'timestamp': datetime.utcnow().isoformat(),
                    'source': 'ovapi',
                    'version': '1.0',
                    'host': self.name,
                    'queue': queue_key
                }
            }

            # Serialize message
            message_body = json.dumps(message_with_metadata, default=str)

            # Send message to SQS
            send_params = {
                'QueueUrl': queue_url,
                'MessageBody': message_body
            }

            if delay_seconds > 0:
                send_params['DelaySeconds'] = delay_seconds

            self.client.send_message(**send_params)

            self._logger.info("Successfully published message to queue '%s' on host '%s'", queue_key, self.name)
            return True

        except Exception as e:
            self._logger.error("Failed to publish message to queue '%s' on host '%s': %s", queue_key, self.name, str(e))
            return False

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on SQS connection

        Returns:
            Dict containing health status and details
        """
        try:
            if not self._ensure_connection():
                return {
                    'status': 'unhealthy',
                    'message': f'Cannot establish connection to SQS provider "{self.provider}" on host "{self.name}"',
                    'provider': self.provider,
                    'host': self.name
                }

            # Test connection with list_queues
            self.client.list_queues()

            return {
                'status': 'healthy',
                'message': f'SQS connection is healthy for provider "{self.provider}" on host "{self.name}"',
                'provider': self.provider,
                'host': self.name,
                'queues': list(self.queues.keys())
            }

        except Exception as e:
            self._logger.error("SQS health check failed for '%s': %s", self.name, str(e))
            return {
                'status': 'unhealthy',
                'message': f'SQS health check failed for provider "{self.provider}" on host "{self.name}": {str(e)}',
                'provider': self.provider,
                'host': self.name
            }

    def close(self):
        """Close SQS connection"""
        try:
            if self.client:
                self.client = None
            self._queue_url_cache.clear()
            self._logger.info("SQS connection closed for host '%s'", self.name)
        except Exception as e:
            self._logger.error("Error closing SQS connection for '%s': %s", self.name, str(e))


class SQSService:
    """
    Production-quality SQS service with multi-provider connection management,
    queue management, and message operations.
    """

    def __init__(self, flask_app=None):
        self.app = flask_app
        self.connections: Dict[str, SQSConnection] = {}
        self._logger = logging.getLogger(__name__)

        if flask_app:
            self._configure_from_app(flask_app)

    def _configure_from_app(self, flask_app):
        """Configure SQS connections from Flask app config (multi-host JSON only)"""
        hosts_json = flask_app.config.get('SQS_HOSTS_CONFIG')
        hosts: List[Dict[str, Any]] = []
        if hosts_json:
            try:
                parsed = json.loads(hosts_json)
                if not isinstance(parsed, list):
                    raise ValueError("SQS_HOSTS_CONFIG must be a JSON array")
                hosts = parsed
            except (json.JSONDecodeError, ValueError) as e:
                self._logger.error("Invalid SQS_HOSTS_CONFIG: %s", e)
        else:
            self._logger.warning("SQS_HOSTS_CONFIG not set; no SQS hosts configured")

        # Global defaults
        default_provider = flask_app.config.get('SQS_PROVIDER', 'aws')
        default_region_name = flask_app.config.get('SQS_REGION_NAME', 'us-east-1')
        default_use_ssl = flask_app.config.get('SQS_USE_SSL', True)
        default_verify_ssl = flask_app.config.get('SQS_VERIFY_SSL', True)
        default_max_attempts = int(flask_app.config.get('SQS_MAX_ATTEMPTS', '3'))
        default_connect_timeout = int(flask_app.config.get('SQS_CONNECT_TIMEOUT', '60'))
        default_read_timeout = int(flask_app.config.get('SQS_READ_TIMEOUT', '60'))
        default_retry_mode = flask_app.config.get('SQS_RETRY_MODE', 'standard')

        built_hosts: List[Dict[str, Any]] = []
        for host in hosts:
            name = host.get('name') or host.get('NAME_ID')
            if not name:
                self._logger.error("SQS host config missing 'name' (NAME_ID)")
                continue

            endpoint_url = host.get('endpoint_url') or host.get('SQS_ENDPOINT_URL')
            region_name = host.get('region_name') or host.get('SQS_REGION_NAME') or default_region_name
            access_key_id = host.get('access_key_id') or host.get('SQS_ACCESS_KEY_ID') or ''
            secret_access_key = host.get('secret_access_key') or host.get('SQS_SECRET_ACCESS_KEY') or ''

            # Region is always required, but credentials can be empty for IAM role usage
            if not region_name:
                self._logger.error(
                    "SQS host '%s' missing required field: region_name",
                    name,
                )
                continue

            provider = (host.get('provider') or host.get('SQS_PROVIDER') or default_provider).lower()
            use_ssl = bool(host.get('use_ssl') if 'use_ssl' in host else host.get('SQS_USE_SSL', default_use_ssl))
            verify_ssl = bool(host.get('verify_ssl') if 'verify_ssl' in host else host.get('SQS_VERIFY_SSL', default_verify_ssl))
            max_attempts = int(host.get('max_attempts') or host.get('SQS_MAX_ATTEMPTS') or default_max_attempts)
            connect_timeout = int(host.get('connect_timeout') or host.get('SQS_CONNECT_TIMEOUT') or default_connect_timeout)
            read_timeout = int(host.get('read_timeout') or host.get('SQS_READ_TIMEOUT') or default_read_timeout)
            retry_mode = host.get('retry_mode') or host.get('SQS_RETRY_MODE') or default_retry_mode

            # Queue configurations (queue_key -> queue config)
            queues_config = host.get('queues', {})
            queues: Dict[str, Dict[str, Any]] = {}
            if isinstance(queues_config, dict):
                for qkey, qconf in queues_config.items():
                    if isinstance(qconf, str):
                        # Simple format: just queue name
                        queues[qkey] = {'name': qconf}
                    elif isinstance(qconf, dict):
                        # Full format with optional attributes
                        queues[qkey] = {
                            'name': qconf.get('name') or qconf.get('queue_name') or qkey
                        }
                    else:
                        self._logger.warning("Invalid queue config for key '%s' on host '%s'", qkey, name)

            built_hosts.append({
                'name': name,
                'provider': provider,
                'endpoint_url': endpoint_url,
                'region_name': region_name,
                'access_key_id': access_key_id,
                'secret_access_key': secret_access_key,
                'use_ssl': use_ssl,
                'verify_ssl': verify_ssl,
                'max_attempts': max_attempts,
                'connect_timeout': connect_timeout,
                'read_timeout': read_timeout,
                'retry_mode': retry_mode,
                'queues': queues,
            })

        # Create connections for each host
        for host in built_hosts:
            name = host.get('name', f"host_{host.get('provider', 'aws')}")
            self.connections[name] = SQSConnection(name, host)

    def connect(self) -> bool:
        """
        Establish connections to all SQS hosts

        Returns:
            bool: True if at least one connection successful, False otherwise
        """
        success_count = 0
        total_connections = len(self.connections)

        for _name, connection in self.connections.items():
            if connection.connect():
                success_count += 1
                # Setup queues for this connection
                connection.setup_queues()

        self._logger.info("Connected to %s/%s SQS hosts", success_count, total_connections)
        return success_count > 0

    def _ensure_connection(self, host_name: str = None) -> bool:
        """
        Ensure connection is established and healthy for a specific host or any host

        Args:
            host_name: Specific host name to check, or None for any available host

        Returns:
            bool: True if connection is healthy, False otherwise
        """
        if host_name:
            if host_name not in self.connections:
                self._logger.error("Host '%s' not found in connections", host_name)
                return False
            return self.connections[host_name]._ensure_connection()

        # Check any available connection
        for connection in self.connections.values():
            if connection._ensure_connection():
                return True

        return False

    def _find_host_for_queue_key(self, queue_key: str) -> Optional[str]:
        """Find the host that defines the given queue_key in its configuration."""
        for host, connection in self.connections.items():
            if queue_key in connection.queues:
                return host
        return None

    def publish_message(self, queue_key: str, message: Dict[str, Any], delay_seconds: int = 0, host_name: str = None) -> bool:
        """
        Publish a message to a specific SQS queue

        Args:
            queue_key: Key of the queue configuration to publish to
            message: Message data to publish
            delay_seconds: Optional delay in seconds before message becomes available
            host_name: Specific host to use, or None for any available host

        Returns:
            bool: True if message published successfully, False otherwise
        """
        # If host_name is not explicitly provided, try to infer from queue_key
        inferred_host = None
        if not host_name:
            inferred_host = self._find_host_for_queue_key(queue_key)

        target_host = host_name or inferred_host

        if target_host:
            # Publish to specific host
            if target_host not in self.connections:
                self._logger.error("Host '%s' not found in connections", target_host)
                return False

            return self.connections[target_host].publish_message(queue_key, message, delay_seconds)

        # Try any available connection
        for connection in self.connections.values():
            if connection.publish_message(queue_key, message, delay_seconds):
                return True

        self._logger.error("Failed to publish message to queue '%s' on any host", queue_key)
        return False

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on all SQS connections

        Returns:
            Dict containing health status and details for all hosts
        """
        health_results = {}
        overall_status = 'healthy'
        failed_hosts = []

        for name, connection in self.connections.items():
            host_health = connection.health_check()
            health_results[name] = host_health

            if host_health['status'] == 'unhealthy':
                overall_status = 'unhealthy'
                failed_hosts.append(name)

        return {
            'status': overall_status,
            'message': f'SQS health check completed. Failed hosts: {failed_hosts}' if failed_hosts else 'All SQS connections are healthy',
            'hosts': health_results,
            'total_hosts': len(self.connections),
            'healthy_hosts': len(self.connections) - len(failed_hosts),
            'failed_hosts': failed_hosts
        }

    def get_available_hosts(self) -> List[str]:
        """
        Get list of available (healthy) host names

        Returns:
            List of available host names
        """
        available_hosts = []
        for name, connection in self.connections.items():
            if connection._ensure_connection():
                available_hosts.append(name)
        return available_hosts

    def get_host_queues(self, host_name: str) -> Dict[str, Any]:
        """
        Get queue configurations for a specific host

        Args:
            host_name: Name of the host

        Returns:
            Dict containing queue configurations for the host
        """
        if host_name not in self.connections:
            return {}

        return self.connections[host_name].queues

    def close(self):
        """Close all SQS connections"""
        for connection in self.connections.values():
            connection.close()
        self._logger.info("All SQS connections closed")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
