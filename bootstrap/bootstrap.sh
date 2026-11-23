#!/usr/bin/env bash
# One-shot bootstrap for a fresh stack. Runs inside the `bootstrap` compose
# service (trinodb/trino image, which ships curl and the trino CLI).
#
# Creates what the Bronze pipeline needs before its first run:
#   1. the MinIO bucket that holds the Iceberg warehouse
#   2. the Iceberg schema for Bronze
#
# Idempotent: safe to run on every `docker compose up`.
set -euo pipefail

MINIO_URL="http://minio:9000"
TRINO_URL="http://trino:8080"
BUCKET="warehouse"
SCHEMA="iceberg.bronze"
SCHEMA_LOCATION="s3a://${BUCKET}/"

: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is not set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is not set}"

echo "[bootstrap] waiting for MinIO at ${MINIO_URL}"
for _ in $(seq 1 30); do
  if curl -sf -o /dev/null "${MINIO_URL}/minio/health/live"; then
    break
  fi
  sleep 2
done
curl -sf -o /dev/null "${MINIO_URL}/minio/health/live" \
  || { echo "[bootstrap] MinIO is not reachable" >&2; exit 1; }

# S3 CreateBucket, signed with AWS SigV4. 200 = created,
# 409 = BucketAlreadyOwnedByYou (already there), both are fine.
echo "[bootstrap] creating bucket '${BUCKET}'"
status=$(curl -s -o /tmp/create-bucket.xml -w '%{http_code}' \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}" \
  -X PUT "${MINIO_URL}/${BUCKET}")
case "${status}" in
  200) echo "[bootstrap] bucket '${BUCKET}' created" ;;
  409) echo "[bootstrap] bucket '${BUCKET}' already exists" ;;
  *)   echo "[bootstrap] creating bucket failed: HTTP ${status}" >&2
       cat /tmp/create-bucket.xml >&2
       exit 1 ;;
esac

echo "[bootstrap] creating schema ${SCHEMA} at ${SCHEMA_LOCATION}"
trino --server "${TRINO_URL}" --user bootstrap --execute \
  "CREATE SCHEMA IF NOT EXISTS ${SCHEMA} WITH (location = '${SCHEMA_LOCATION}')"

echo "[bootstrap] schemas in iceberg:"
trino --server "${TRINO_URL}" --user bootstrap --execute "SHOW SCHEMAS FROM iceberg"

echo "[bootstrap] done"
