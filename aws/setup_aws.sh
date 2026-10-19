#!/usr/bin/env bash
# One-time AWS setup for the CFPB pipeline: the raw-data bucket. Safe to re-run.
#
# Prereqs: AWS CLI v2, signed in with `aws login`.
#   BUCKET=cfpb-complaints-xyz REGION=us-east-2 ./aws/setup_aws.sh
#
# There is deliberately no IAM role for Snowflake: see the note in
# snowflake/setup.sql (Snowflake loads through an internal stage instead).
set -euo pipefail

: "${BUCKET:?set BUCKET}"
REGION="${REGION:-us-east-2}"
HERE="$(cd "$(dirname "$0")" && pwd)"
# On Git Bash for Windows, aws.exe needs C:/... paths for file:// arguments.
HERE_NATIVE="$(cygpath -m "$HERE" 2>/dev/null || echo "$HERE")"

echo "== Bucket s3://$BUCKET ($REGION)"
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
  else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
fi
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration "file://$HERE_NATIVE/lifecycle.json"
echo "Done."
