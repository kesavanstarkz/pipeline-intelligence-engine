#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy-pipeline.sh
# One-shot script to deploy the AWS CodePipeline testing stack.
#
# Prerequisites
#   • AWS CLI v2 configured (aws configure)
#   • A CodeStar Connection to GitHub already created & "Available"
#   • The repo pushed to GitHub
#
# Usage
#   chmod +x scripts/deploy-pipeline.sh
#   ./scripts/deploy-pipeline.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Config — edit these ───────────────────────────────────────────────────────
GITHUB_OWNER="${GITHUB_OWNER:-YOUR_GITHUB_USERNAME}"
GITHUB_REPO="${GITHUB_REPO:-pipeline-intelligence-engine}"
GITHUB_BRANCH="${GITHUB_BRANCH:-main}"
CODESTAR_CONNECTION_ARN="${CODESTAR_CONNECTION_ARN:-}"   # required
AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="pipeline-intelligence-engine-ci"
TEMPLATE_FILE="infra/codepipeline.yml"

# ── Validate ──────────────────────────────────────────────────────────────────
if [[ -z "$CODESTAR_CONNECTION_ARN" ]]; then
  echo "ERROR: Set CODESTAR_CONNECTION_ARN environment variable."
  echo "       Create a connection in the AWS Console:"
  echo "       CodePipeline → Settings → Connections → Create connection → GitHub"
  exit 1
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Deploying: $STACK_NAME"
echo " Region   : $AWS_REGION"
echo " Repo     : $GITHUB_OWNER/$GITHUB_REPO@$GITHUB_BRANCH"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

aws cloudformation deploy \
  --region "$AWS_REGION" \
  --template-file "$TEMPLATE_FILE" \
  --stack-name "$STACK_NAME" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      GitHubOwner="$GITHUB_OWNER" \
      GitHubRepo="$GITHUB_REPO" \
      GitHubBranch="$GITHUB_BRANCH" \
      GitHubConnectionArn="$CODESTAR_CONNECTION_ARN"

echo ""
echo "✅  Stack deployed successfully."
echo ""

# Print console URLs from stack outputs
aws cloudformation describe-stacks \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[*].[OutputKey,OutputValue]" \
  --output table
