#!/usr/bin/env bash

# Packages:
# pip install -U pip bandit pylint pytest pytest-cov pytest-flask pytest-json-report pytest-metadata pytest-mock pytest-xdist

set -euo pipefail

PROJECT_ROOT="$(pwd)"
ENV_FILE="$PROJECT_ROOT/tests/envs.sh"

CONTAINERS=("test-ov-aws-sqs" "test-ov-dynamodb" "test-ov-minio")

# shellcheck source=/dev/null
source "$ENV_FILE"

# --- Helper functions ---
container_exists() {
    docker container inspect "$1" &>/dev/null
}

container_running() {
    [[ "$(docker container inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]
}

start_container() {
    local name="$1"
    if container_running "$name"; then
        echo "$name is already running."
    elif container_exists "$name"; then
        echo "Starting existing container $name..."
        docker start "$name"
    else
        return 1
    fi
    return 0
}

# --- Cleanup ---
cleanup() {
    echo "Stopping and removing test containers..."
    for name in "${CONTAINERS[@]}"; do
        if container_exists "$name"; then
            echo "  Removing $name..."
            docker rm -f "$name" &>/dev/null || true
        else
            echo "  $name does not exist, skipping."
        fi
    done
    echo "Cleanup complete."
}

if [[ "${1:-}" == "cleanup" ]]; then
    cleanup
    exit 0
fi

# --- Start SQS (LocalStack) ---
# Default port 4566 -> 24566
echo "=== AWS SQS (LocalStack) ==="
if ! start_container "test-ov-aws-sqs"; then
    echo "Creating test-ov-aws-sqs..."
    docker run -d \
        --name test-ov-aws-sqs \
        -p 54566:4566 \
        -e "SERVICES=sqs" \
        -e "AWS_DEFAULT_REGION=us-east-1" \
        -e "AWS_ACCESS_KEY_ID=$SQS_ACCESS_KEY_ID" \
        -e "AWS_SECRET_ACCESS_KEY=$SQS_SECRET_ACCESS_KEY" \
        localstack/localstack
fi

# --- Start DynamoDB Local ---
# Default port 8000 -> 28000
echo "=== DynamoDB Local ==="
if ! start_container "test-ov-dynamodb"; then
    echo "Creating test-ov-dynamodb..."
    docker run -d \
        --name test-ov-dynamodb \
        -p 58000:8000 \
        amazon/dynamodb-local \
        -jar DynamoDBLocal.jar -sharedDb
fi

# --- Start Minio ---
# Default ports 9000 -> 29000, 9001 -> 29001
echo "=== Minio ==="
if ! start_container "test-ov-minio"; then
    echo "Creating test-ov-minio..."
    docker run -d \
        --name test-ov-minio \
        -p 59000:9000 \
        -p 59001:9001 \
        -e "MINIO_ROOT_USER=$S3SITE01_ACCESS_KEY_ID" \
        -e "MINIO_ROOT_PASSWORD=$S3SITE01_SECRET_ACCESS_KEY" \
        minio/minio server /data --console-address ":9001"
fi

# ===========================================================
# Config Setup - runs after containers are up
# ===========================================================

export AWS_ACCESS_KEY_ID="$DYNAMODB_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$DYNAMODB_SECRET_KEY"
export AWS_DEFAULT_REGION="$DYNAMODB_REGION"

DYNAMODB_ENDPOINT="http://localhost:58000"

# --- Wait for DynamoDB to be ready ---
echo ""
echo "=== Config Setup ==="
echo "Waiting for DynamoDB to be ready..."
for i in $(seq 1 30); do
    if aws dynamodb list-tables --endpoint-url "$DYNAMODB_ENDPOINT" --region "$AWS_DEFAULT_REGION" &>/dev/null; then
        break
    fi
    sleep 1
done

# --- DynamoDB Tables ---
create_dynamodb_table() {
    local table_name="$1"
    local key_name="$2"
    local key_type="${3:-S}"

    if aws dynamodb describe-table \
        --table-name "$table_name" \
        --endpoint-url "$DYNAMODB_ENDPOINT" \
        --region "$AWS_DEFAULT_REGION" &>/dev/null; then
        echo "  DynamoDB table '$table_name' already exists."
    else
        echo "  Creating DynamoDB table '$table_name'..."
        aws dynamodb create-table \
            --table-name "$table_name" \
            --attribute-definitions "AttributeName=$key_name,AttributeType=$key_type" \
            --key-schema "AttributeName=$key_name,KeyType=HASH" \
            --billing-mode PAY_PER_REQUEST \
            --endpoint-url "$DYNAMODB_ENDPOINT" \
            --region "$AWS_DEFAULT_REGION" > /dev/null
    fi
}

create_dynamodb_table "foobars" "key"

echo ""
echo "All test containers are running."
