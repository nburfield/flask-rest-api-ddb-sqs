"""
S3 Service for handling object storage operations with multi-provider support
"""

import json
import logging
import threading
from typing import Dict, Any, Optional, List

import boto3
from botocore.exceptions import (
    ClientError,
)
# Note: Older boto3/botocore versions expose only S3UploadFailedError; download failures raise ClientError.
from botocore.config import Config


class S3Connection:  # pylint: disable=too-many-instance-attributes
    """
    Individual S3 connection wrapper supporting multiple providers
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
        self.access_key_id = config['access_key_id']
        self.secret_access_key = config['secret_access_key']
        # Session tokens are no longer supported
        self.session_token = None

        # Client configuration
        self.max_attempts = config.get('max_attempts', 3)
        self.connect_timeout = config.get('connect_timeout', 60)
        self.read_timeout = config.get('read_timeout', 60)
        self.retry_mode = config.get('retry_mode', 'standard')

        # Bucket configurations (kept for compatibility; callers pass bucket names directly)
        self.buckets = config.get('buckets', {})

        # Signed URL configuration
        self.signed_url_expiration = config.get('signed_url_expiration', 3600)  # 1 hour default
        self.signed_url_content_disposition = config.get('signed_url_content_disposition', 'attachment')

    def connect(self) -> bool:
        """
        Establish connection to S3 with automatic retry logic

        Returns:
            bool: True if connection successful, False otherwise
        """
        with self._connection_lock:
            try:
                if self.client:
                    return True

                self._logger.info("Connecting to S3 provider '%s' on host '%s'...", self.provider, self.name)

                # Configure client based on provider
                client_config = Config(
                    region_name=self.region_name,
                    retries={'max_attempts': self.max_attempts},
                    connect_timeout=self.connect_timeout,
                    read_timeout=self.read_timeout,
                    parameter_validation=False
                )

                # Create session
                # For AWS provider, check if using IAM role (placeholder values)
                # If placeholders are detected, use None to let boto3 use IAM role credentials
                use_iam_role = (
                    self.provider == 'aws' and
                    self.access_key_id in ['AWS_S3_ACCESS_KEY_PLACEHOLDER', 'IAM_ROLE'] and
                    self.secret_access_key in ['AWS_S3_SECRET_KEY_PLACEHOLDER', 'IAM_ROLE']
                )
                
                if use_iam_role:
                    # Use IAM role credentials (boto3 will automatically use Lambda's IAM role)
                    session = boto3.Session()
                else:
                    session = boto3.Session(
                        aws_access_key_id=self.access_key_id,
                        aws_secret_access_key=self.secret_access_key
                    )

                # Create client based on provider
                if self.provider == 'aws':
                    self.client = session.client('s3', config=client_config)
                elif self.provider in ['minio', 'linode', 'custom']:
                    self.client = session.client(
                        's3',
                        endpoint_url=self.endpoint_url,
                        config=client_config,
                        use_ssl=self.use_ssl,
                        verify=self.verify_ssl
                    )
                else:
                    raise ValueError(f"Unsupported S3 provider: {self.provider}")

                # Test connection
                self.client.list_buckets()

                # Reset reconnect delay on successful connection
                self._reconnect_delay = 1
                self._logger.info("Successfully connected to S3 provider '%s' on host '%s'", self.provider, self.name)
                return True

            except Exception as e:
                self._logger.error("Failed to connect to S3 provider '%s' on host '%s': %s", self.provider, self.name, str(e))
                self._schedule_reconnect()
                return False

    def _schedule_reconnect(self):
        """Schedule a reconnection attempt with exponential backoff"""
        if self._reconnect_delay < self._max_reconnect_delay:
            self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

        self._logger.info("Scheduling reconnection attempt for '%s' in %s seconds", self.name, self._reconnect_delay)
        threading.Timer(self._reconnect_delay, self._reconnect).start()

    def _reconnect(self):
        """Attempt to reconnect to S3"""
        if not self.connect():
            self._schedule_reconnect()

    def _strip_auth_prefix(self, bucket_name: str) -> str:
        """Strip '<NAME_ID>.' prefix from bucket if present."""
        prefix = f"{self.name}."
        if isinstance(bucket_name, str) and bucket_name.startswith(prefix):
            return bucket_name[len(prefix):]
        return bucket_name

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
            self.client.list_buckets()
            return True

        except Exception as e:
            self._logger.error("Connection health check failed for '%s': %s", self.name, str(e))
            return self.connect()

    def _get_bucket_region(self, bucket_name: str) -> str:
        """
        Get the region where a bucket is located
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            str: Region name (defaults to self.region_name if detection fails)
        """
        if not self._ensure_connection():
            return self.region_name
            
        try:
            actual_bucket = self._strip_auth_prefix(bucket_name)
            
            # get_bucket_location must be called from us-east-1 endpoint for accurate results
            # Create a temporary client in us-east-1 to query bucket location
            us_east_config = Config(
                region_name='us-east-1',
                retries={'max_attempts': self.max_attempts},
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
                parameter_validation=False
            )
            
            use_iam_role = (
                self.provider == 'aws' and
                self.access_key_id in ['AWS_S3_ACCESS_KEY_PLACEHOLDER', 'IAM_ROLE'] and
                self.secret_access_key in ['AWS_S3_SECRET_KEY_PLACEHOLDER', 'IAM_ROLE']
            )
            
            if use_iam_role:
                session = boto3.Session()
            else:
                session = boto3.Session(
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.secret_access_key
                )
            
            us_east_client = session.client('s3', config=us_east_config)
            
            # Get bucket location - returns None for us-east-1, 'EU' for Europe, or region name
            response = us_east_client.get_bucket_location(Bucket=actual_bucket)
            location = response.get('LocationConstraint')
            
            # Handle special cases: None means us-east-1, 'EU' means eu-west-1
            if location is None:
                detected_region = 'us-east-1'
            elif location == 'EU':
                detected_region = 'eu-west-1'
            else:
                detected_region = location
            
            self._logger.info("Detected bucket '%s' region: %s", actual_bucket, detected_region)
            return detected_region
        except Exception as e:
            self._logger.warning("Failed to get bucket region for '%s', using configured region '%s': %s", bucket_name, self.region_name, str(e))
            return self.region_name

    def _get_region_client(self, region_name: str, for_presigned_url: bool = False):
        """
        Get an S3 client configured for a specific region
        
        Args:
            region_name: AWS region name
            for_presigned_url: If True, configure client specifically for presigned URL generation
            
        Returns:
            boto3.client: S3 client configured for the region
        """
        # Always create a fresh client for the specific region to ensure
        # presigned URLs are generated with the correct region-specific endpoint
        client_config = Config(
            region_name=region_name,
            retries={'max_attempts': self.max_attempts},
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout,
            parameter_validation=False,
            signature_version='s3v4',
            s3={
                'addressing_style': 'virtual'  # Use virtual-hosted-style URLs (bucket.s3.region.amazonaws.com)
            }
        )
        
        use_iam_role = (
            self.provider == 'aws' and
            self.access_key_id in ['AWS_S3_ACCESS_KEY_PLACEHOLDER', 'IAM_ROLE'] and
            self.secret_access_key in ['AWS_S3_SECRET_KEY_PLACEHOLDER', 'IAM_ROLE']
        )
        
        if use_iam_role:
            session = boto3.Session(region_name=region_name)  # Set region in session
        else:
            session = boto3.Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=region_name  # Set region in session
            )
        
        # For presigned URLs, explicitly set the endpoint URL to force region-specific endpoint
        # This ensures the presigned URL is generated with the correct endpoint and valid signature
        if for_presigned_url:
            # Use the region-specific endpoint format: s3.region.amazonaws.com
            # This format works for both virtual-hosted-style and path-style URLs
            endpoint_url = f'https://s3.{region_name}.amazonaws.com'
            region_client = session.client('s3', endpoint_url=endpoint_url, config=client_config)
            self._logger.debug("Created S3 client for region: %s with endpoint: %s (for presigned URLs)", region_name, endpoint_url)
        else:
            # For regular operations, let boto3 determine the endpoint
            region_client = session.client('s3', config=client_config)
            self._logger.debug("Created S3 client for region: %s", region_name)
        
        return region_client

    def get_signed_url(self, bucket_name: str, object_key: str, operation: str = 'get_object',
                       *, expiration: int = None, content_disposition: str = None, null_if_not_exists: bool = False) -> Optional[str]:
        """
        Generate a signed URL for S3 operations

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            operation: S3 operation ('get_object', 'put_object', 'delete_object')
            expiration: URL expiration time in seconds
            content_disposition: Content disposition header
            null_if_not_exists: If True, return None when object doesn't exist

        Returns:
            str: Signed URL or None if failed or object doesn't exist (when null_if_not_exists=True)
        """
        if not self._ensure_connection():
            self._logger.error("Cannot generate signed URL for '%s': no connection", self.name)
            return None

        # Check if object exists when null_if_not_exists is True
        if null_if_not_exists and not self.object_exists(bucket_name, object_key):
            return None

        try:
            # Set default values
            expiration = expiration or self.signed_url_expiration
            content_disposition = content_disposition or self.signed_url_content_disposition

            # Strip auth prefix from bucket
            actual_bucket = self._strip_auth_prefix(bucket_name)
            
            # Get the bucket's actual region and use a client configured for that region
            # This ensures presigned URLs are generated with the correct region
            bucket_region = self._get_bucket_region(actual_bucket)
            # Create client specifically configured for presigned URL generation with explicit endpoint
            region_client = self._get_region_client(bucket_region, for_presigned_url=True)
            
            # Generate signed URL using region-specific client
            if operation == 'get_object':
                url = region_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': actual_bucket,
                        'Key': object_key,
                        'ResponseContentDisposition': content_disposition
                    },
                    ExpiresIn=expiration
                )
            elif operation == 'put_object':
                url = region_client.generate_presigned_url(
                    'put_object',
                    Params={
                        'Bucket': actual_bucket,
                        'Key': object_key,
                        'ContentType': 'application/octet-stream'
                    },
                    ExpiresIn=expiration
                )
            elif operation == 'delete_object':
                url = region_client.generate_presigned_url(
                    'delete_object',
                    Params={
                        'Bucket': actual_bucket,
                        'Key': object_key
                    },
                    ExpiresIn=expiration
                )
            else:
                raise ValueError(f"Unsupported operation: {operation}")

            self._logger.info("Generated %s signed URL for '%s' in bucket '%s' (region: %s) on '%s'", operation, object_key, actual_bucket, bucket_region, self.name)
            return url

        except Exception as e:
            self._logger.error("Failed to generate signed URL for '%s' in bucket '%s' on '%s': %s", object_key, bucket_name, self.name, str(e))
            return None

    def get_signed_put_url(self, bucket_name: str, object_key: str, *, _host_name: str = None,
                           expiration: int = None, content_type: str = 'application/octet-stream') -> Optional[str]:
        """
        Generate a signed PUT URL (only if object doesn't exist)

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            _host_name: Specific host to use, or None for any available host (unused in this implementation)
            expiration: URL expiration time in seconds
            content_type: Content type for the object

        Returns:
            str: Signed PUT URL or None if object exists or failed
        """
        if not self._ensure_connection():
            self._logger.error("Cannot generate signed PUT URL for '%s': no connection", self.name)
            return None

        try:
            # Strip auth prefix from bucket
            actual_bucket = self._strip_auth_prefix(bucket_name)
            
            # Get the bucket's actual region
            bucket_region = self._get_bucket_region(actual_bucket)
            # Use regular client for head_object check
            check_client = self._get_region_client(bucket_region, for_presigned_url=False)
            
            # Check if object exists
            try:
                check_client.head_object(Bucket=actual_bucket, Key=object_key)
                self._logger.warning("Object '%s' already exists in bucket '%s' on '%s'", object_key, actual_bucket, self.name)
                return None
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # Object doesn't exist, generate PUT URL
                    # Use client configured specifically for presigned URL generation
                    expiration = expiration or self.signed_url_expiration
                    presigned_client = self._get_region_client(bucket_region, for_presigned_url=True)

                    url = presigned_client.generate_presigned_url(
                        'put_object',
                        Params={
                            'Bucket': actual_bucket,
                            'Key': object_key,
                            'ContentType': content_type
                        },
                        ExpiresIn=expiration
                    )
                    
                    self._logger.info("Generated PUT signed URL for '%s' in bucket '%s' (region: %s) on '%s'", object_key, actual_bucket, bucket_region, self.name)
                    return url
                raise

        except Exception as e:
            self._logger.error("Failed to generate signed PUT URL for '%s' in bucket '%s' on '%s': %s", object_key, bucket_name, self.name, str(e))
            return None

    def copy_object(self, source_bucket: str, source_key: str, dest_bucket: str, *, dest_key: str) -> bool:
        """
        Copy an object within the same S3 provider

        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            dest_bucket: Destination bucket name
            dest_key: Destination object key

        Returns:
            bool: True if copy successful, False otherwise
        """
        if not self._ensure_connection():
            self._logger.error("Cannot copy object for '%s': no connection", self.name)
            return False

        try:
            # Copy object
            actual_source_bucket = self._strip_auth_prefix(source_bucket)
            actual_dest_bucket = self._strip_auth_prefix(dest_bucket)
            copy_source = {'Bucket': actual_source_bucket, 'Key': source_key}
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=actual_dest_bucket,
                Key=dest_key
            )

            self._logger.info(
                "Successfully copied object from '%s' to '%s' on '%s'",
                f"{actual_source_bucket}/{source_key}",
                f"{actual_dest_bucket}/{dest_key}",
                self.name,
            )
            return True

        except Exception as e:
            self._logger.error(
                "Failed to copy object from '%s' to '%s' on '%s': %s",
                f"{source_bucket}/{source_key}",
                f"{dest_bucket}/{dest_key}",
                self.name,
                str(e),
            )
            return False

    def object_exists(self, bucket_name: str, object_key: str) -> bool:
        """
        Check if an object exists in the specified bucket

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object

        Returns:
            bool: True if object exists, False otherwise
        """
        if not self._ensure_connection():
            self._logger.error("Cannot check object existence for '%s': no connection", self.name)
            return False

        try:
            actual_bucket = self._strip_auth_prefix(bucket_name)
            self.client.head_object(Bucket=actual_bucket, Key=object_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                self._logger.error(
                    "Error checking object existence for '%s' in bucket '%s' on '%s': %s",
                    object_key,
                    bucket_name,
                    self.name,
                    str(e),
                )
            return False
        except Exception as e:
            self._logger.error(
                "Failed to check object existence for '%s' in bucket '%s' on '%s': %s",
                object_key,
                bucket_name,
                self.name,
                str(e),
            )
            return False

    def get_object_metadata(self, bucket_name: str, object_key: str) -> Optional[Dict[str, Any]]:
        """
        Get object metadata

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object

        Returns:
            Dict containing object metadata or None if failed
        """
        if not self._ensure_connection():
            self._logger.error("Cannot get object metadata for '%s': no connection", self.name)
            return None

        try:
            actual_bucket = self._strip_auth_prefix(bucket_name)
            response = self.client.head_object(Bucket=actual_bucket, Key=object_key)

            metadata = {
                'content_length': response.get('ContentLength'),
                'content_type': response.get('ContentType'),
                'last_modified': response.get('LastModified'),
                'etag': response.get('ETag'),
                'metadata': response.get('Metadata', {})
            }

            self._logger.info("Retrieved metadata for '%s' in bucket '%s' on '%s'", object_key, actual_bucket, self.name)
            return metadata

        except Exception as e:
            self._logger.error("Failed to get metadata for '%s' in bucket '%s' on '%s': %s", object_key, bucket_name, self.name, str(e))
            return None

    def list_objects(self, bucket_name: str, prefix: str = '', max_keys: int = 1000) -> List[Dict[str, Any]]:
        """
        List objects in a bucket with optional prefix

        Args:
            bucket_name: Name of the bucket
            prefix: Object key prefix to filter by
            max_keys: Maximum number of keys to return

        Returns:
            List of object information dictionaries
        """
        if not self._ensure_connection():
            self._logger.error("Cannot list objects for '%s': no connection", self.name)
            return []

        try:
            actual_bucket = self._strip_auth_prefix(bucket_name)
            response = self.client.list_objects_v2(
                Bucket=actual_bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )

            objects = []
            for obj in response.get('Contents', []):
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'etag': obj['ETag']
                })

            self._logger.info(
                "Listed %s objects in bucket '%s' with prefix '%s' on '%s'",
                len(objects),
                actual_bucket,
                prefix,
                self.name,
            )
            return objects

        except Exception as e:
            self._logger.error(
                "Failed to list objects in bucket '%s' with prefix '%s' on '%s': %s",
                bucket_name,
                prefix,
                self.name,
                str(e),
            )
            return []

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on S3 connection

        Returns:
            Dict containing health status and details
        """
        try:
            if not self._ensure_connection():
                return {
                    'status': 'unhealthy',
                    'message': f'Cannot establish connection to S3 provider "{self.provider}" on host "{self.name}"',
                    'provider': self.provider,
                    'host': self.name
                }

            # Test connection with list_buckets
            self.client.list_buckets()

            return {
                'status': 'healthy',
                'message': f'S3 connection is healthy for provider "{self.provider}" on host "{self.name}"',
                'provider': self.provider,
                'host': self.name,
                'buckets': list(self.buckets.keys())
            }

        except Exception as e:
            self._logger.error("S3 health check failed for '%s': %s", self.name, str(e))
            return {
                'status': 'unhealthy',
                'message': f'S3 health check failed for provider "{self.provider}" on host "{self.name}": {str(e)}',
                'provider': self.provider,
                'host': self.name
            }

    def close(self):
        """Close S3 connection"""
        try:
            if self.client:
                self.client = None
            self._logger.info("S3 connection closed for host '%s'", self.name)
        except Exception as e:
            self._logger.error("Error closing S3 connection for '%s': %s", self.name, str(e))


class S3Service:
    """
    Production-quality S3 service with multi-provider connection management,
    bucket management, and signed URL operations.
    """

    def __init__(self, flask_app=None):
        self.app = flask_app
        self.connections: Dict[str, S3Connection] = {}
        self._logger = logging.getLogger(__name__)

        if flask_app:
            self._configure_from_app(flask_app)

    def _configure_from_app(self, flask_app):
        """Configure S3 connections from Flask app config (multi-host JSON only)"""
        hosts_json = flask_app.config.get('S3_HOSTS_CONFIG')
        hosts: List[Dict[str, Any]] = []
        if hosts_json:
            try:
                parsed = json.loads(hosts_json)
                if not isinstance(parsed, list):
                    raise ValueError("S3_HOSTS_CONFIG must be a JSON array")
                hosts = parsed
            except (json.JSONDecodeError, ValueError) as e:
                self._logger.error("Invalid S3_HOSTS_CONFIG: %s", e)
        else:
            self._logger.warning("S3_HOSTS_CONFIG not set; no S3 hosts configured")

        # Global defaults
        default_provider = flask_app.config.get('S3_PROVIDER', 'aws')
        default_region_name = flask_app.config.get('S3_REGION_NAME', 'us-east-1')
        default_use_ssl = flask_app.config.get('S3_USE_SSL', True)
        default_verify_ssl = flask_app.config.get('S3_VERIFY_SSL', True)
        default_max_attempts = int(flask_app.config.get('S3_MAX_ATTEMPTS', '3'))
        default_connect_timeout = int(flask_app.config.get('S3_CONNECT_TIMEOUT', '60'))
        default_read_timeout = int(flask_app.config.get('S3_READ_TIMEOUT', '60'))
        default_retry_mode = flask_app.config.get('S3_RETRY_MODE', 'standard')
        default_signed_url_expiration = int(flask_app.config.get('S3_SIGNED_URL_EXPIRATION', '3600'))
        default_signed_url_content_disposition = flask_app.config.get('S3_SIGNED_URL_CONTENT_DISPOSITION', 'attachment')

        built_hosts: List[Dict[str, Any]] = []
        for host in hosts:
            name = host.get('name') or host.get('NAME_ID')
            if not name:
                self._logger.error("S3 host config missing 'name' (NAME_ID)")
                continue

            endpoint_url = host.get('endpoint_url') or host.get('S3_ENDPOINT_URL') or ''
            region_name = host.get('region_name') or host.get('S3_REGION_NAME') or default_region_name
            # Handle both lowercase and uppercase keys
            access_key_id = host.get('access_key_id') or host.get('S3_ACCESS_KEY_ID')
            secret_access_key = host.get('secret_access_key') or host.get('S3_SECRET_ACCESS_KEY')
            provider = (host.get('provider') or host.get('S3_PROVIDER') or default_provider).lower()
            
            # For AWS provider, endpoint_url is optional and credentials can be IAM role placeholders
            if provider == 'aws':
                if not region_name:
                    self._logger.error("S3 host '%s' missing required field: region_name", name)
                    continue
                # For AWS with IAM roles, access keys can be placeholders or empty (boto3 will use IAM role)
                # Allow placeholder values for IAM role authentication
                iam_role_placeholders = ['AWS_S3_ACCESS_KEY_PLACEHOLDER', 'AWS_S3_SECRET_KEY_PLACEHOLDER', 'IAM_ROLE', '']
                is_iam_role_auth = (
                    (access_key_id in iam_role_placeholders or not access_key_id) and
                    (secret_access_key in iam_role_placeholders or not secret_access_key)
                )
                
                # If not using IAM role, both credentials must be provided and valid
                if not is_iam_role_auth:
                    if not access_key_id or not secret_access_key:
                        self._logger.error("S3 host '%s' missing required fields: access_key_id/secret_access_key", name)
                        continue
                
                # If using IAM role (placeholders or empty), set placeholders for connect() method
                if is_iam_role_auth:
                    if not access_key_id:
                        access_key_id = 'AWS_S3_ACCESS_KEY_PLACEHOLDER'
                    if not secret_access_key:
                        secret_access_key = 'AWS_S3_SECRET_KEY_PLACEHOLDER'
            else:
                # For non-AWS providers, all fields are required
                if not all([endpoint_url, region_name, access_key_id, secret_access_key]):
                    self._logger.error(
                        "S3 host '%s' missing required fields (endpoint_url/region/access_key_id/secret_access_key)",
                        name,
                    )
                    continue
            # Session tokens are not supported
            use_ssl = bool(host.get('use_ssl') if 'use_ssl' in host else host.get('S3_USE_SSL', default_use_ssl))
            verify_ssl = bool(host.get('verify_ssl') if 'verify_ssl' in host else host.get('S3_VERIFY_SSL', default_verify_ssl))
            max_attempts = int(host.get('max_attempts') or host.get('S3_MAX_ATTEMPTS') or default_max_attempts)
            connect_timeout = int(host.get('connect_timeout') or host.get('S3_CONNECT_TIMEOUT') or default_connect_timeout)
            read_timeout = int(host.get('read_timeout') or host.get('S3_READ_TIMEOUT') or default_read_timeout)
            retry_mode = host.get('retry_mode') or host.get('S3_RETRY_MODE') or default_retry_mode
            signed_url_expiration = int(host.get('signed_url_expiration') or host.get('S3_SIGNED_URL_EXPIRATION') or default_signed_url_expiration)
            signed_url_content_disposition = (host.get('signed_url_content_disposition') or
                                            host.get('S3_SIGNED_URL_CONTENT_DISPOSITION') or
                                            default_signed_url_content_disposition)

            # Buckets are not defined in config; callers provide bucket names
            buckets: Dict[str, Any] = {}

            built_hosts.append({
                'name': name,
                'provider': provider,
                'endpoint_url': endpoint_url,
                'region_name': region_name,
                'access_key_id': access_key_id,
                'secret_access_key': secret_access_key,
                # Session tokens removed
                'use_ssl': use_ssl,
                'verify_ssl': verify_ssl,
                'max_attempts': max_attempts,
                'connect_timeout': connect_timeout,
                'read_timeout': read_timeout,
                'retry_mode': retry_mode,
                'signed_url_expiration': signed_url_expiration,
                'signed_url_content_disposition': signed_url_content_disposition,
                'buckets': buckets,
            })

        # Create connections for each host
        for host in built_hosts:
            name = host.get('name', f"host_{host.get('provider', 'aws')}")
            self.connections[name] = S3Connection(name, host)

    def connect(self) -> bool:
        """
        Establish connections to all S3 hosts

        Returns:
            bool: True if at least one connection successful, False otherwise
        """
        success_count = 0
        total_connections = len(self.connections)

        for _name, connection in self.connections.items():
            if connection.connect():
                success_count += 1

        self._logger.info("Connected to %s/%s S3 hosts", success_count, total_connections)
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

    def _parse_host_from_bucket(self, bucket_name: str) -> Optional[str]:
        if not bucket_name or '.' not in bucket_name:
            return None
        candidate = bucket_name.split('.', 1)[0]
        return candidate if candidate in self.connections else None

    def get_signed_url(self, bucket_name: str, object_key: str, operation: str = 'get_object',
                       *, expiration: int = None, content_disposition: str = None, null_if_not_exists: bool = False) -> Optional[str]:  # pylint: disable=too-many-arguments,too-many-positional-arguments
        """
        Get a signed URL for S3 operations

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            operation: S3 operation ('get_object', 'put_object', 'delete_object')
            expiration: URL expiration time in seconds
            content_disposition: Content disposition header
            null_if_not_exists: If True, return None when object doesn't exist

        Returns:
            str: Signed URL or None if failed or object doesn't exist (when null_if_not_exists=True)
        """
        inferred_host = self._parse_host_from_bucket(bucket_name)
        if inferred_host:
            if inferred_host not in self.connections:
                self._logger.error("Host '%s' not found in connections", inferred_host)
                return None
            return self.connections[inferred_host].get_signed_url(bucket_name, object_key, operation, expiration=expiration, content_disposition=content_disposition, null_if_not_exists=null_if_not_exists)

        # Try any available connection
        for connection in self.connections.values():
            url = connection.get_signed_url(bucket_name, object_key, operation, expiration=expiration, content_disposition=content_disposition, null_if_not_exists=null_if_not_exists)
            if url:
                return url

        return None

    def get_signed_put_url(self, bucket_name: str, object_key: str, *, host_name: str = None,
                           expiration: int = None, content_type: str = 'application/octet-stream') -> Optional[str]:
        """
        Get a signed PUT URL (only if object doesn't exist)

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            host_name: Specific host to use, or None for any available host
            expiration: URL expiration time in seconds
            content_type: Content type for the object

        Returns:
            str: Signed PUT URL or None if object exists or failed
        """
        inferred_host = host_name or self._parse_host_from_bucket(bucket_name)
        if inferred_host:
            if host_name not in self.connections:
                self._logger.error("Host '%s' not found in connections", host_name)
                return None
            return self.connections[inferred_host].get_signed_put_url(bucket_name, object_key, expiration=expiration, content_type=content_type)

        # Try any available connection
        for connection in self.connections.values():
            url = connection.get_signed_put_url(bucket_name, object_key, expiration=expiration, content_type=content_type)
            if url:
                return url

        return None

    def copy_object(self, source_bucket: str, source_key: str, dest_bucket: str, *, dest_key: str,
                    host_name: str = None) -> bool:  # pylint: disable=too-many-arguments,too-many-positional-arguments
        """
        Copy an object within the same S3 provider

        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            dest_bucket: Destination bucket name
            dest_key: Destination object key
            host_name: Specific host to use, or None for any available host

        Returns:
            bool: True if copy successful, False otherwise
        """
        inferred_host = host_name or self._parse_host_from_bucket(source_bucket) or self._parse_host_from_bucket(dest_bucket)
        if inferred_host:
            if host_name not in self.connections:
                self._logger.error("Host '%s' not found in connections", host_name)
                return False
            return self.connections[inferred_host].copy_object(source_bucket, source_key, dest_bucket, dest_key=dest_key)

        # Try any available connection
        for connection in self.connections.values():
            if connection.copy_object(source_bucket, source_key, dest_bucket, dest_key=dest_key):
                return True

        return False

    def object_exists(self, bucket_name: str, object_key: str, host_name: str = None) -> bool:
        """
        Check if an object exists in the specified bucket

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            host_name: Specific host to use, or None for any available host

        Returns:
            bool: True if object exists, False otherwise
        """
        inferred_host = host_name or self._parse_host_from_bucket(bucket_name)
        if inferred_host:
            if host_name not in self.connections:
                self._logger.error("Host '%s' not found in connections", host_name)
                return False
            return self.connections[inferred_host].object_exists(bucket_name, object_key)

        # Try any available connection
        for connection in self.connections.values():
            if connection.object_exists(bucket_name, object_key):
                return True

        return False

    def get_object_metadata(self, bucket_name: str, object_key: str, host_name: str = None) -> Optional[Dict[str, Any]]:
        """
        Get object metadata

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            host_name: Specific host to use, or None for any available host

        Returns:
            Dict containing object metadata or None if failed
        """
        inferred_host = host_name or self._parse_host_from_bucket(bucket_name)
        if inferred_host:
            if host_name not in self.connections:
                self._logger.error("Host '%s' not found in connections", host_name)
                return None
            return self.connections[inferred_host].get_object_metadata(bucket_name, object_key)

        # Try any available connection
        for connection in self.connections.values():
            metadata = connection.get_object_metadata(bucket_name, object_key)
            if metadata:
                return metadata

        return None

    def list_objects(self, bucket_name: str, prefix: str = '', max_keys: int = 1000, host_name: str = None) -> List[Dict[str, Any]]:
        """
        List objects in a bucket with optional prefix

        Args:
            bucket_name: Name of the bucket
            prefix: Object key prefix to filter by
            max_keys: Maximum number of keys to return
            host_name: Specific host to use, or None for any available host

        Returns:
            List of object information dictionaries
        """
        inferred_host = host_name or self._parse_host_from_bucket(bucket_name)
        if inferred_host:
            if host_name not in self.connections:
                self._logger.error("Host '%s' not found in connections", host_name)
                return []
            return self.connections[inferred_host].list_objects(bucket_name, prefix, max_keys)

        # Try any available connection
        for connection in self.connections.values():
            objects = connection.list_objects(bucket_name, prefix, max_keys)
            if objects:
                return objects

        return []

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on all S3 connections

        Returns:
            Dict containing health status and details for all hosts
        """
        health_results = {}
        overall_status = 'healthy'
        failed_hosts = []

        for name, connection in self.connections.items():  # pylint: disable=too-many-locals
            host_health = connection.health_check()
            health_results[name] = host_health

            if host_health['status'] == 'unhealthy':
                overall_status = 'unhealthy'
                failed_hosts.append(name)

        return {
            'status': overall_status,
            'message': f'S3 health check completed. Failed hosts: {failed_hosts}' if failed_hosts else 'All S3 connections are healthy',
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

    def get_host_buckets(self, host_name: str) -> Dict[str, Any]:
        """
        Get bucket configurations for a specific host

        Args:
            host_name: Name of the host

        Returns:
            Dict containing bucket configurations for the host
        """
        if host_name not in self.connections:
            return {}

        return self.connections[host_name].buckets

    def close(self):
        """Close all S3 connections"""
        for connection in self.connections.values():
            connection.close()
        self._logger.info("All S3 connections closed")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
