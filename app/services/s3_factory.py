"""
S3 Factory for managing S3 service instances
"""

from app.services.s3_service import S3Service


class S3Factory:
    """
    Factory class for managing S3 service instances.
    Provides a singleton pattern for the S3 service.
    """

    _instance = None
    _configured = False

    @classmethod
    def configure(cls, flask_app):
        """
        Configure the S3 factory with Flask app

        Args:
            flask_app: Flask application instance
        """
        if not cls._configured:
            cls._instance = S3Service(flask_app)
            cls._configured = True

    @classmethod
    def get_service(cls) -> S3Service:
        """
        Get the configured S3 service instance

        Returns:
            S3Service: Configured S3 service instance

        Raises:
            RuntimeError: If factory is not configured
        """
        if not cls._configured or not cls._instance:
            raise RuntimeError("S3Factory not configured. Call configure() first.")

        return cls._instance

    @classmethod
    def get_connection(cls, host_name: str = None):
        """
        Get a specific S3 connection or any available connection

        Args:
            host_name: Specific host name, or None for any available connection

        Returns:
            S3Connection: The requested connection

        Raises:
            RuntimeError: If factory is not configured or connection not found
        """
        service = cls.get_service()

        if host_name:
            if host_name not in service.connections:
                raise RuntimeError(f"Host '{host_name}' not found in S3 connections")
            return service.connections[host_name]

        # Return any available connection
        available_hosts = service.get_available_hosts()
        if not available_hosts:
            raise RuntimeError("No available S3 connections")

        return service.connections[available_hosts[0]]

    @classmethod
    def get_available_hosts(cls) -> list:
        """
        Get list of available (healthy) host names

        Returns:
            list: List of available host names
        """
        service = cls.get_service()
        return service.get_available_hosts()

    @classmethod
    def health_check(cls) -> dict:
        """
        Perform health check on S3 service

        Returns:
            dict: Health check results
        """
        try:
            service = cls.get_service()
            return service.health_check()
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'S3 service not available: {str(e)}'
            }

    @classmethod
    def get_signed_url(cls, bucket_name: str, object_key: str, *, operation: str = 'get_object',
                       host_name: str = None, expiration: int = None, content_disposition: str = None, null_if_not_exists: bool = False) -> str:
        """
        Get a signed URL for S3 operations

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            operation: S3 operation ('get_object', 'put_object', 'delete_object')
            host_name: Specific host to use, or None for any available host
            expiration: URL expiration time in seconds
            content_disposition: Content disposition header
            null_if_not_exists: If True, return None when object doesn't exist

        Returns:
            str: Signed URL or None if object doesn't exist (when null_if_not_exists=True)

        Raises:
            RuntimeError: If factory is not configured or operation fails
        """
        service = cls.get_service()
        url = service.get_signed_url(bucket_name, object_key, operation, expiration=expiration, content_disposition=content_disposition, null_if_not_exists=null_if_not_exists)
        if not url and not null_if_not_exists:
            raise RuntimeError(f"Failed to generate signed URL for {operation} on {object_key}")
        return url

    @classmethod
    def get_signed_put_url(cls, bucket_name: str, object_key: str, *, host_name: str = None,
                           expiration: int = None, content_type: str = 'application/octet-stream') -> str:
        """
        Get a signed PUT URL (only if object doesn't exist)

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            host_name: Specific host to use, or None for any available host
            expiration: URL expiration time in seconds
            content_type: Content type for the object

        Returns:
            str: Signed PUT URL

        Raises:
            RuntimeError: If factory is not configured, object exists, or operation fails
        """
        service = cls.get_service()
        url = service.get_signed_put_url(bucket_name, object_key, host_name=host_name, expiration=expiration, content_type=content_type)
        if not url:
            raise RuntimeError(f"Failed to generate signed PUT URL for {object_key} (object may already exist)")
        return url

    @classmethod
    def copy_object(cls, source_bucket: str, source_key: str, dest_bucket: str, *, dest_key: str,
                    host_name: str = None) -> bool:
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
        service = cls.get_service()
        return service.copy_object(source_bucket, source_key, dest_bucket, dest_key, host_name)

    @classmethod
    def object_exists(cls, bucket_name: str, object_key: str, host_name: str = None) -> bool:
        """
        Check if an object exists in the specified bucket

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            host_name: Specific host to use, or None for any available host

        Returns:
            bool: True if object exists, False otherwise
        """
        service = cls.get_service()
        return service.object_exists(bucket_name, object_key, host_name)

    @classmethod
    def get_object_metadata(cls, bucket_name: str, object_key: str, host_name: str = None) -> dict:
        """
        Get object metadata

        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
            host_name: Specific host to use, or None for any available host

        Returns:
            dict: Object metadata

        Raises:
            RuntimeError: If factory is not configured or metadata retrieval fails
        """
        service = cls.get_service()
        metadata = service.get_object_metadata(bucket_name, object_key, host_name)
        if not metadata:
            raise RuntimeError(f"Failed to get metadata for {object_key}")
        return metadata

    @classmethod
    def list_objects(cls, bucket_name: str, prefix: str = '', max_keys: int = 1000, host_name: str = None) -> list:
        """
        List objects in a bucket with optional prefix

        Args:
            bucket_name: Name of the bucket
            prefix: Object key prefix to filter by
            max_keys: Maximum number of keys to return
            host_name: Specific host to use, or None for any available host

        Returns:
            list: List of object information dictionaries
        """
        service = cls.get_service()
        return service.list_objects(bucket_name, prefix, max_keys, host_name)

    @classmethod
    def get_host_buckets(cls, host_name: str) -> dict:
        """
        Get bucket configurations for a specific host

        Args:
            host_name: Name of the host

        Returns:
            dict: Bucket configurations for the host
        """
        service = cls.get_service()
        return service.get_host_buckets(host_name)

    @classmethod
    def close(cls):
        """Close the S3 service connection"""
        if cls._instance:
            cls._instance.close()
            cls._instance = None
            cls._configured = False
