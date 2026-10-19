#!/usr/bin/env bash
# One-time AWS setup for the CFPB pipeline. Safe to re-run.
#
# Prereqs: AWS CLI v2, signed in with `aws login`.
#   BUCKET=cfpb-complaints-xyz REGION=us-east-2 ./aws/setup_aws.sh bucket
#   BUCKET=cfpb-complaints-xyz ./aws/setup_aws.sh pipeline-user
#
# There is deliberately no IAM role for Snowflake: see the note in
# snowflake/setup.sql (Snowflake loads through an internal stage instead).
set -euo pipefail

: "${BUCKET:?set BUCKET}"
REGION="${REGION:-us-east-2}"
HERE="$(cd "$(dirname "$0")" && pwd)"
# On Git Bash for Windows, aws.exe needs C:/... paths for file:// arguments.
HERE_NATIVE="$(cygpath -m "$HERE" 2>/dev/null || echo "$HERE")"
PIPELINE_USER="cfpb-pipeline"

bucket() {
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
}

# IAM user for unattended (scheduled) runs. `aws login` sessions end after at most
# 12 hours, so the Airflow container needs a credential that doesn't expire. This
# user can only list the bucket and read/write raw/cfpb/ and state/.
pipeline_user() {
  echo "== IAM user $PIPELINE_USER"
  aws iam get-user --user-name "$PIPELINE_USER" >/dev/null 2>&1 || \
    aws iam create-user --user-name "$PIPELINE_USER" >/dev/null
  aws iam put-user-policy --user-name "$PIPELINE_USER" --policy-name cfpb-pipeline-s3 \
    --policy-document "$(sed "s|__BUCKET__|$BUCKET|g" "$HERE/pipeline-user-policy.json")"
  echo "Done. Now create an access key for $PIPELINE_USER yourself (console: IAM > Users >"
  echo "$PIPELINE_USER > Security credentials > Create access key) and store it with:"
  echo "  aws configure --profile $PIPELINE_USER"
  echo "then set PIPELINE_AWS_PROFILE=$PIPELINE_USER in .env and restart Airflow."
}

case "${1:-bucket}" in
  bucket) bucket ;;
  pipeline-user) pipeline_user ;;
  *) echo "usage: $0 [bucket|pipeline-user]" >&2; exit 2 ;;
esac
