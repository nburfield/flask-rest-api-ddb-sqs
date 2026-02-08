"""
SQS Factory for managing SQS service instances
"""

from app.services.sqs_service import SQSService


class SQSFactory:
    """
    Factory class for managing SQS service instances.
    Provides a singleton pattern for the SQS service.
    """

    _instance = None
    _configured = False

    @classmethod
    def configure(cls, flask_app):
        """
        Configure the SQS factory with Flask app

        Args:
            flask_app: Flask application instance
        """
        if not cls._configured:
            cls._instance = SQSService(flask_app)
            # Establish connections immediately on configuration
            try:
                cls._instance.connect()
            except Exception:
                # Swallow exceptions here to avoid breaking app startup; health endpoints
                # can surface issues and reconnect logic will retry.
                pass
            cls._configured = True

    @classmethod
    def get_service(cls) -> SQSService:
        """
        Get the configured SQS service instance

        Returns:
            SQSService: Configured SQS service instance

        Raises:
            RuntimeError: If factory is not configured
        """
        if not cls._configured or not cls._instance:
            raise RuntimeError("SQSFactory not configured. Call configure() first.")

        return cls._instance

    @classmethod
    def get_connection(cls, host_name: str = None):
        """
        Get a specific SQS connection or any available connection

        Args:
            host_name: Specific host name, or None for any available connection

        Returns:
            SQSConnection: The requested connection

        Raises:
            RuntimeError: If factory is not configured or connection not found
        """
        service = cls.get_service()

        if host_name:
            if host_name not in service.connections:
                raise RuntimeError(f"Host '{host_name}' not found in SQS connections")
            return service.connections[host_name]

        # Return any available connection
        available_hosts = service.get_available_hosts()
        if not available_hosts:
            raise RuntimeError("No available SQS connections")

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
        Perform health check on SQS service

        Returns:
            dict: Health check results
        """
        try:
            service = cls.get_service()
            return service.health_check()
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'SQS service not available: {str(e)}'
            }

    @classmethod
    def publish_message(cls, queue_key: str, message: dict, delay_seconds: int = 0, host_name: str = None) -> bool:
        """
        Publish a message to a specific SQS queue

        Args:
            queue_key: Key of the queue configuration to publish to
            message: Message data to publish
            delay_seconds: Optional delay in seconds before message becomes available
            host_name: Specific host to use, or None for any available host

        Returns:
            bool: True if message published successfully, False otherwise

        Raises:
            RuntimeError: If factory is not configured or operation fails
        """
        service = cls.get_service()
        success = service.publish_message(queue_key, message, delay_seconds=delay_seconds, host_name=host_name)
        if not success:
            raise RuntimeError(f"Failed to publish message to queue '{queue_key}'")
        return success

    @classmethod
    def close(cls):
        """Close the SQS service connection"""
        if cls._instance:
            cls._instance.close()
            cls._instance = None
            cls._configured = False
