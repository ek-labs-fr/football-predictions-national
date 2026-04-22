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

### 4. Install Docker Desktop and have it running

`IngestStack` packages the Lambda as a container image (`DockerImageFunction`),
so Docker is needed **before you bootstrap** — `cdk bootstrap` runs
`python app.py` first to collect context, which loads the stack and therefore
touches Docker during asset preparation. Without Docker, bootstrap hangs
silently.

Install from https://www.docker.com/products/docker-desktop/, launch it, and
wait for the whale icon in the system tray to go steady. Then verify:

```bash
docker --version
docker info
```

### 5. Bootstrap CDK in your account/region

This creates the CDK deployment bucket, ECR repo for Lambda images, and IAM
roles. Run once per (account, region) pair.

```bash
cd infrastructure
cdk bootstrap aws://<ACCOUNT_ID>/<REGION>
# Example: cdk bootstrap aws://123456789012/eu-west-3
```

### 6. Store the API-Football key in Secrets Manager

Must be created in the **same region** as the stack. The Lambda reads the
secret on cold start and caches it in memory.

**bash / macOS / Linux:**

```bash
aws secretsmanager create-secret \
  --name "football-predictions/api-football-key" \
  --description "API-Football v3 key for daily ingest Lambda" \
  --secret-string '{"api_key":"YOUR_API_FOOTBALL_KEY"}' \
  --region eu-west-3
```

**PowerShell (Windows)** — avoids leaking the key to shell history and
sidesteps nested-quote issues:

```powershell
$key = Read-Host -AsSecureString "Paste your API-Football key"
$plain = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($key)
)
$json = @{ api_key = $plain } | ConvertTo-Json -Compress
$tmp = New-TemporaryFile
Set-Content -Path $tmp -Value $json -Encoding ASCII -NoNewline

aws secretsmanager create-secret `
  --name "football-predictions/api-football-key" `
  --description "API-Football v3 key for daily ingest Lambda" `
  --secret-string "file://$tmp" `
  --region eu-west-3

Remove-Item $tmp
Remove-Variable key, plain, json, tmp
```

## Troubleshooting

### `cdk bootstrap` hangs with no output

Happens when Docker isn't installed/running, because `cdk bootstrap` synths
the app first and `IngestStack`'s `DockerImageFunction` asset preparation
blocks on the Docker daemon. Fix: install Docker Desktop (step 4 above).

If you need to bootstrap on a machine without Docker (e.g. CI that only
deploys the toolkit), skip the CDK CLI and deploy the bootstrap template
directly. The `CDKToolkit` stack is fully described by a static template
that CDK can emit via `--show-template`:

```powershell
# 1. Emit the template using a minimal dummy app (bypasses IngestStack)
#    Save this to bootstrap_app.py, next to cdk.json:
#      import aws_cdk as cdk
#      cdk.App().synth()

cdk bootstrap --show-template --app "python bootstrap_app.py" `
  | Out-File -Encoding ASCII bootstrap.yaml

# 2. Deploy the CDKToolkit stack directly via CloudFormation
aws cloudformation create-stack `
  --stack-name CDKToolkit `
  --template-body file://bootstrap.yaml `
  --parameters ParameterKey=Qualifier,ParameterValue=hnb659fds `
  --capabilities CAPABILITY_NAMED_IAM `
  --region eu-west-3

aws cloudformation wait stack-create-complete `
  --stack-name CDKToolkit --region eu-west-3
```

`hnb659fds` is the default CDK bootstrap qualifier; `cdk deploy` expects
this unless you override it with `--qualifier`.

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
