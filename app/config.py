"""
Configuration objects for the Flask application.
Sets the data that can be accessed with app.config["key"]
"""

import os
import uuid
import json


class BaseConfig:
    # pylint: disable=too-few-public-methods
    """Base configuration"""

    SECRET_KEY = os.environ.get('SECRET_KEY')
    JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY')

    DATABASE_BACKEND = 'dynamo'.lower()
    DYNAMODB_REGION = os.environ.get('DYNAMODB_REGION')
    DYNAMODB_ENDPOINT = os.environ.get('DYNAMODB_ENDPOINT')
    DYNAMODB_ACCESS_KEY = os.environ.get('DYNAMODB_ACCESS_KEY')
    DYNAMODB_SECRET_KEY = os.environ.get('DYNAMODB_SECRET_KEY')

    # RabbitMQ Configuration
    RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', '5672'))
    RABBITMQ_VIRTUAL_HOST = os.environ.get('RABBITMQ_VIRTUAL_HOST', '/')
    RABBITMQ_CONNECTION_TIMEOUT = int(os.environ.get('RABBITMQ_CONNECTION_TIMEOUT', '30'))
    RABBITMQ_HEARTBEAT_INTERVAL = int(os.environ.get('RABBITMQ_HEARTBEAT_INTERVAL', '600'))
    RABBITMQ_BLOCKED_CONNECTION_TIMEOUT = int(os.environ.get('RABBITMQ_BLOCKED_CONNECTION_TIMEOUT', '300'))

    # RabbitMQ Multi-Host Configuration (always-on)
    _raw_rabbitmq_hosts_config = os.environ.get('RABBITMQ_HOSTS_CONFIG')
    RABBITMQ_HOSTS_CONFIG = '[]'
    if _raw_rabbitmq_hosts_config:
        try:
            parsed_hosts = json.loads(_raw_rabbitmq_hosts_config)
            # Handle potential double-encoded JSON
            if isinstance(parsed_hosts, str):
                parsed_hosts = json.loads(parsed_hosts)
            if isinstance(parsed_hosts, list):
                for host in parsed_hosts:
                    if not isinstance(host, dict):
                        continue
                    username_env_key = host.get('RABBITMQ_USERNAME')
                    password_env_key = host.get('RABBITMQ_PASSWORD')
                    if isinstance(username_env_key, str):
                        host['RABBITMQ_USERNAME'] = os.environ.get(username_env_key)
                    if isinstance(password_env_key, str):
                        host['RABBITMQ_PASSWORD'] = os.environ.get(password_env_key)
                RABBITMQ_HOSTS_CONFIG = json.dumps(parsed_hosts)
        except Exception:
            RABBITMQ_HOSTS_CONFIG = '[]'

    # S3 Configuration - Single Host (Legacy)
    S3_ACCESS_KEY_ID = os.environ.get('S3_ACCESS_KEY_ID')
    S3_SECRET_ACCESS_KEY = os.environ.get('S3_SECRET_ACCESS_KEY')
    S3_USE_SSL = os.environ.get('S3_USE_SSL', 'true').lower() == 'true'
    S3_VERIFY_SSL = os.environ.get('S3_VERIFY_SSL', 'true').lower() == 'true'
    S3_MAX_ATTEMPTS = int(os.environ.get('S3_MAX_ATTEMPTS', '3'))
    S3_CONNECT_TIMEOUT = int(os.environ.get('S3_CONNECT_TIMEOUT', '60'))
    S3_READ_TIMEOUT = int(os.environ.get('S3_READ_TIMEOUT', '60'))
    S3_RETRY_MODE = os.environ.get('S3_RETRY_MODE', 'standard')
    S3_SIGNED_URL_EXPIRATION = int(os.environ.get('S3_SIGNED_URL_EXPIRATION', '3600'))
    S3_SIGNED_URL_CONTENT_DISPOSITION = os.environ.get('S3_SIGNED_URL_CONTENT_DISPOSITION', 'attachment')

    # S3 Multi-Host Configuration (always-on)
    _raw_s3_hosts_config = os.environ.get('S3_HOSTS_CONFIG')
    S3_HOSTS_CONFIG = '[]'
    if _raw_s3_hosts_config:
        try:
            parsed_hosts = json.loads(_raw_s3_hosts_config)
            # Handle potential double-encoded JSON
            if isinstance(parsed_hosts, str):
                parsed_hosts = json.loads(parsed_hosts)
            if isinstance(parsed_hosts, list):
                for host in parsed_hosts:
                    if not isinstance(host, dict):
                        continue
                    access_key_id_env_key = host.get('S3_ACCESS_KEY_ID')
                    secret_access_key_env_key = host.get('S3_SECRET_ACCESS_KEY')
                    if isinstance(access_key_id_env_key, str):
                        host['S3_ACCESS_KEY_ID'] = os.environ.get(access_key_id_env_key)
                    if isinstance(secret_access_key_env_key, str):
                        host['S3_SECRET_ACCESS_KEY'] = os.environ.get(secret_access_key_env_key)
                S3_HOSTS_CONFIG = json.dumps(parsed_hosts)
        except Exception:
            S3_HOSTS_CONFIG = '[]'

    # SQS Configuration - Single Host (Legacy)
    SQS_PROVIDER = os.environ.get('SQS_PROVIDER', 'aws')
    SQS_REGION_NAME = os.environ.get('SQS_REGION_NAME', 'us-east-1')
    SQS_USE_SSL = os.environ.get('SQS_USE_SSL', 'true').lower() == 'true'
    SQS_VERIFY_SSL = os.environ.get('SQS_VERIFY_SSL', 'true').lower() == 'true'
    SQS_MAX_ATTEMPTS = int(os.environ.get('SQS_MAX_ATTEMPTS', '3'))
    SQS_CONNECT_TIMEOUT = int(os.environ.get('SQS_CONNECT_TIMEOUT', '60'))
    SQS_READ_TIMEOUT = int(os.environ.get('SQS_READ_TIMEOUT', '60'))
    SQS_RETRY_MODE = os.environ.get('SQS_RETRY_MODE', 'standard')

    # SQS Multi-Host Configuration (always-on)
    _raw_sqs_hosts_config = os.environ.get('SQS_HOSTS_CONFIG')
    SQS_HOSTS_CONFIG = '[]'
    if _raw_sqs_hosts_config:
        try:
            parsed_hosts = json.loads(_raw_sqs_hosts_config)
            # Handle potential double-encoded JSON
            if isinstance(parsed_hosts, str):
                parsed_hosts = json.loads(parsed_hosts)
            if isinstance(parsed_hosts, list):
                for host in parsed_hosts:
                    if not isinstance(host, dict):
                        continue
                    access_key_id_env_key = host.get('SQS_ACCESS_KEY_ID')
                    secret_access_key_env_key = host.get('SQS_SECRET_ACCESS_KEY')
                    # Only resolve as environment variable if it's a non-empty string
                    # Empty strings are preserved (used for IAM role authentication)
                    if isinstance(access_key_id_env_key, str) and access_key_id_env_key:
                        resolved = os.environ.get(access_key_id_env_key)
                        if resolved is not None:
                            host['SQS_ACCESS_KEY_ID'] = resolved
                    if isinstance(secret_access_key_env_key, str) and secret_access_key_env_key:
                        resolved = os.environ.get(secret_access_key_env_key)
                        if resolved is not None:
                            host['SQS_SECRET_ACCESS_KEY'] = resolved
                SQS_HOSTS_CONFIG = json.dumps(parsed_hosts)
        except Exception:
            SQS_HOSTS_CONFIG = '[]'

    OTP_ISSUER = os.environ.get('OTP_ISSUER')

    POD_UID = os.environ.get('POD_UID')


class DevelopmentConfig(BaseConfig):
    # pylint: disable=too-few-public-methods
    """Development configuration"""

    # security config
    FLASK_DEBUG = True
    POD_UID = str(uuid.uuid4())


class QAConfig(BaseConfig):
    # pylint: disable=too-few-public-methods
    """Development configuration"""

    # security config
    FLASK_DEBUG = False


class ProductionConfig(BaseConfig):
    # pylint: disable=too-few-public-methods
    """Production configuration"""

    # security config
    FLASK_DEBUG = False
