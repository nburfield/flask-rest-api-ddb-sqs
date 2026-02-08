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
        -p 94566:4566 \
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
        -p 98000:8000 \
        amazon/dynamodb-local \
        -jar DynamoDBLocal.jar -sharedDb -dbPath /home/dynamodblocal/data
fi

# --- Start Minio ---
# Default ports 9000 -> 29000, 9001 -> 29001
echo "=== Minio ==="
if ! start_container "test-ov-minio"; then
    echo "Creating test-ov-minio..."
    docker run -d \
        --name test-ov-minio \
        -p 99000:9000 \
        -p 99001:9001 \
        -e "MINIO_ROOT_USER=$S3SITE01_ACCESS_KEY_ID" \
        -e "MINIO_ROOT_PASSWORD=$S3SITE01_SECRET_ACCESS_KEY" \
        minio/minio server /data --console-address ":9001"
fi

echo ""
echo "All test containers are running."
