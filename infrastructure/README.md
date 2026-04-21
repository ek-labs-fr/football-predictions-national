# Infrastructure — AWS Deployment

CDK stack for the daily incremental data ingestion pipeline.

## Stacks

| Stack | Purpose |
|---|---|
| `FPIngestStack` | S3 bucket + Lambda + Step Functions + EventBridge daily rule (06:00 UTC) |

## One-off setup (first time only)

These steps must be run once per AWS account/region. You need to do them — I can't.

### 1. Install the AWS CLI and configure credentials

```bash
# Install (Windows — via MSI)
# https://awscli.amazonaws.com/AWSCLIV2.msi

aws configure
# AWS Access Key ID:     <paste from IAM console>
# AWS Secret Access Key: <paste>
# Default region:        eu-west-1       # or your preferred region
# Default output:        json

# Verify it works
aws sts get-caller-identity
```

### 2. Install Node.js (required for CDK CLI) and the CDK

```bash
# Node.js 20 LTS: https://nodejs.org/
npm install -g aws-cdk
cdk --version     # should print 2.x
```

### 3. Install the Python CDK libraries

```bash
cd infrastructure
python -m pip install -r requirements.txt
```

### 4. Bootstrap CDK in your account/region

This creates the CDK deployment bucket, ECR repo for Lambda images, and IAM
roles. Run once per (account, region) pair.

```bash
cd infrastructure
cdk bootstrap aws://<ACCOUNT_ID>/<REGION>
# Example: cdk bootstrap aws://123456789012/eu-west-1
```

### 5. Store the API-Football key in Secrets Manager

```bash
aws secretsmanager create-secret \
  --name "football-predictions/api-football-key" \
  --secret-string '{"api_key":"YOUR_API_FOOTBALL_KEY"}'
```

The Lambda reads the secret on cold start and caches it in memory.

### 6. Make sure Docker is running

`cdk deploy` builds the Lambda container image locally using Docker, then
pushes it to ECR. Install Docker Desktop and have it running before the
first deploy.

## Deploy

```bash
cd infrastructure

# First-time or after code changes:
cdk deploy FPIngestStack

# Preview what would change without deploying:
cdk diff FPIngestStack

# Tear everything down (will preserve the S3 bucket due to RETAIN policy):
cdk destroy FPIngestStack
```

## Manually trigger the pipeline (for testing)

```bash
aws stepfunctions start-execution \
  --state-machine-arn $(aws stepfunctions list-state-machines --query "stateMachines[?contains(name,'IngestStateMachine')].stateMachineArn | [0]" --output text) \
  --input '{}'
```

## Verify data is landing

```bash
BUCKET=$(aws cloudformation describe-stacks --stack-name FPIngestStack \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" --output text)

aws s3 ls "s3://$BUCKET/club/fixtures/" --recursive | head -10
aws s3 ls "s3://$BUCKET/national/fixtures/" --recursive | head -10

# Manifest — fixtures whose details have been pulled
aws s3 cp "s3://$BUCKET/club/manifests/fixtures_seen.json" -
```

## Costs

| Resource | Monthly estimate |
|---|---|
| Lambda (30 invocations/mo × 60s × 1GB) | <$0.50 |
| Step Functions (30 executions/mo, Standard) | <$0.10 |
| S3 Standard + IA (<10 GB) | <$0.50 |
| Secrets Manager | $0.40 |
| EventBridge, CloudWatch | free tier |
| **Total** | **~$1–2 / month** |

## Changing scope

To add/remove leagues or change the window, edit the constants in
`src/data/incremental.py`:

```python
CLUB_LEAGUE_SEASONS: list[tuple[int, int]] = [(39, 2025), (61, 2025), (140, 2025)]
NATIONAL_LEAGUE_SEASONS: list[tuple[int, int]] = [(1, 2026), (11, 2026), ...]
WINDOW_BACK_DAYS = 3
WINDOW_FORWARD_DAYS = 7
```

Then `cdk deploy` — CDK rebuilds the Lambda image and swaps it in.
