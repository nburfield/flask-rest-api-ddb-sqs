# Toggle integration tests: set to 1 to run DynamoDB/SQS integration tests when you run pytest.
# Set to 0 or comment out to run only unit/api tests.
export INTEGRATION=1
# If using setup-pytest.sh for containers, set DYNAMODB_ENDPOINT=http://localhost:58000 and SQS endpoint in SQS_HOSTS_CONFIG to http://localhost:54566
export FLASK_APP=app
export FLASK_DEBUG=true
export FLASK_RUN_PORT=8061
export FLASK_RUN_HOST=0.0.0.0
export APP_SETTINGS=app.config.DevelopmentConfig
export DYNAMODB_ENDPOINT='http://localhost:58000'
export DYNAMODB_REGION='us-east-1'
export DYNAMODB_ACCESS_KEY='fakeMyKeyId'
export SQS_ACCESS_KEY_ID='test'
export OTP_ISSUER='OneVizn'

# Should be secret, but dev environment data is safe to publish
export SECRET_KEY=621c135c5f35b25eba8229676acd273f860df0414bf1186f420a1fc17c00cb1b
export JWT_SECRET_KEY=44f132bee4d8e7e1777dcd38ecf63ce9f45b13517b7c126fded43834df86da19
export DYNAMODB_SECRET_KEY='fakeSecretAccessKey'
export SQS_SECRET_ACCESS_KEY='test'
export S3SITE01_ACCESS_KEY_ID=onevizn
export S3SITE01_SECRET_ACCESS_KEY=onevizn123

# S3 multi-host configuration (always used)
# Minimal per-host fields: NAME_ID, S3_ENDPOINT_URL, S3_REGION_NAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
# Optional overrides per host: S3_PROVIDER, S3_USE_SSL, S3_VERIFY_SSL,
# max_attempts, connect_timeout, read_timeout, retry_mode, signed_url_expiration, signed_url_content_disposition
export S3_HOSTS_CONFIG='[
  {
    "NAME_ID": "s3site01",
    "S3_PROVIDER": "minio",
    "S3_ENDPOINT_URL": "http://192.168.0.172:59000",
    "S3_REGION_NAME": "us-east-1",
    "S3_ACCESS_KEY_ID": "S3SITE01_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY": "S3SITE01_SECRET_ACCESS_KEY",
    "S3_USE_SSL": false,
    "S3_VERIFY_SSL": false,
    "buckets": {
      "viznx-one": "viznx-one"
    }
  }
]'

# SQS multi-host configuration (always used)
# Minimal per-host fields: NAME_ID, SQS_REGION_NAME, SQS_ACCESS_KEY_ID, SQS_SECRET_ACCESS_KEY
# Optional overrides per host: SQS_PROVIDER, SQS_ENDPOINT_URL, SQS_USE_SSL, SQS_VERIFY_SSL,
# max_attempts, connect_timeout, read_timeout, retry_mode
export SQS_HOSTS_CONFIG='[
  {
    "NAME_ID": "sqssite01",
    "SQS_PROVIDER": "localstack",
    "SQS_ENDPOINT_URL": "http://localhost:54566",
    "SQS_REGION_NAME": "us-east-1",
    "SQS_ACCESS_KEY_ID": "SQS_ACCESS_KEY_ID",
    "SQS_SECRET_ACCESS_KEY": "SQS_SECRET_ACCESS_KEY",
    "SQS_USE_SSL": false,
    "SQS_VERIFY_SSL": false,
    "queues": {
      "password_reset_requests": {
        "name": "password_reset_requests"
      }
    }
  }
]'
