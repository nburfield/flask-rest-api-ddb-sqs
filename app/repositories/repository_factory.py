"""
A Repository factory to control database type and table access
"""


class RepositoryFactory:
    """Factory class for creating and managing repository instances.

    This factory provides a centralized way to create repository instances
    for different database backends (MongoDB, DynamoDB) and manages their
    configuration and lifecycle.
    """
    _instances = {}
    _backend = None
    _mongo_client = None
    _dynamo_client = None

    @staticmethod
    def get_mongo_client(flask_app):
        """Get MongoDB client instance.

        Args:
            flask_app: Flask application instance with MongoDB configuration

        Returns:
            MongoDB client instance
        """
        # Import locally to avoid hard dependency when MongoDB not used
        from pymongo import MongoClient

        # connect to cluster string
        user = flask_app.config.get("MONGODB_USER", "")
        password = flask_app.config.get("MONGODB_PASSWORD", "")
        server = flask_app.config.get("MONGODB_SERVER", "")
        mongo_uri = f'mongodb://{user}:{password}@{server}'
        return MongoClient(mongo_uri)

    @staticmethod
    def get_dynamodb_client(flask_app):
        """Get DynamoDB client instance.

        Args:
            flask_app: Flask application instance with DynamoDB configuration

        Returns:
            DynamoDB client instance
        """
        # Import locally to avoid hard dependency when DynamoDB not used
        import boto3
        session = boto3.Session(
            aws_access_key_id=flask_app.config.get("DYNAMODB_ACCESS_KEY"),
            aws_secret_access_key=flask_app.config.get("DYNAMODB_SECRET_KEY"),
            region_name=flask_app.config.get("DYNAMODB_REGION", "us-west-2")
        )
        return session.resource("dynamodb", endpoint_url=flask_app.config.get("DYNAMODB_ENDPOINT"))

    @classmethod
    def configure(cls, backend: str, *, mongo_client=None, dynamo_client=None, flask_app=None):
        """Configure the repository factory with backend and clients.

        Args:
            backend: Database backend type ('mongo' or 'dynamo')
            mongo_client: Optional MongoDB client instance
            dynamo_client: Optional DynamoDB client instance
            flask_app: Optional Flask app for database initialization
        """
        cls._backend = backend.lower()
        cls._mongo_client = mongo_client
        cls._dynamo_client = dynamo_client

    @classmethod
    def get(cls, object_type: str, *, table_name=None, key_field="key"):
        """Get or create a repository instance for the specified object type.

        Args:
            object_type: Type of object the repository will handle
            table_name: Optional table name override (for DynamoDB)
            key_field: Field name to use as primary key

        Returns:
            Repository instance for the specified object type

        Raises:
            ValueError: If factory is not configured or backend is unsupported
        """
        if cls._backend is None:
            raise ValueError("RepositoryFactory not configured.")

        if object_type in cls._instances:
            return cls._instances[object_type]

        if cls._backend == "dynamo":
            # Lazy import to avoid importing unused backends at module import time
            from app.repositories.dynamo_repository import DynamoRepository
            repo = DynamoRepository(
                table_name=table_name or object_type,
                key_field=key_field,
                dynamo_client=cls._dynamo_client
            )
        else:
            raise ValueError(f"Unsupported backend: {cls._backend}")

        cls._instances[object_type] = repo
        return repo
