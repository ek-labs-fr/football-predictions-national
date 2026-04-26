# Action Plan — Hybrid CI/CD for Model Training (Option C)

> Status: drafted, not implemented. Deferred until ingestion observability lands.

## Goal

Stand up a training pipeline that an MLOps engineer can recognise on day one,
without the iteration-speed cost of running every training run inside SageMaker.

The shape:

- **Training runs in GitHub Actions** — fast, transparent, easy to debug.
- **Model registry lives in SageMaker** — versioning, approval workflow, lineage,
  and convention familiarity.
- **Inference Lambda resolves the latest "Approved" model at startup** —
  rollback is one console click (or one CLI command).

Not in scope (deferred):

- SageMaker Training Jobs / Pipelines (revisit if training grows past CPU/seconds).
- SageMaker Model Monitor (drift detection — Phase 8 territory, when WC 2026
  resolves enough fixtures to be statistically meaningful).
- Multi-stage promotion (`Dev` → `Staging` → `Prod`). Single-stage approval
  gate is enough for v0; promote directly to `Prod` once approved.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│  GitHub Actions (workflow_dispatch)                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  1. Build training image (Dockerfile.train)              │  │
│  │  2. Pull training_table.csv from S3 (today's snapshot)   │  │
│  │  3. Run train_pipeline.py inside the container           │  │
│  │  4. Compute holdout metrics                              │  │
│  │  5. Register model package in SageMaker Model Registry   │  │
│  │     (status: PendingManualApproval)                      │  │
│  │  6. Optional auto-approve if metric beats current        │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────┬───────────────────────────────────────────┘
                     │ assumes role via OIDC (no static keys)
                     ▼
┌────────────────────────────────────────────────────────────────┐
│  AWS                                                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  S3 data bucket: artefacts/v<ts>/{home,away,scaler}.pkl  │  │
│  │  + rho.json, + manifest.json (metrics, code SHA)         │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  SageMaker Model Package Group (one per mode):           │  │
│  │    fp-national-poisson  ┐                                │  │
│  │    fp-club-poisson      ┴── ModelPackageVersion N        │  │
│  │                              status: Approved | Rejected │  │
│  │                              s3_uri: ↑ artefacts above   │  │
│  │                              metadata: holdout metrics   │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Inference Lambda (FPInferenceStack)                     │  │
│  │    on cold start: list_model_packages (status=Approved,  │  │
│  │      group=fp-{mode}-poisson, sort=desc, limit=1)        │  │
│  │    download .pkl files from the package's S3 URI         │  │
│  │    cache in /tmp for the warm window                     │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

---

## Components to build

### 1. Training image
- `Dockerfile.train` — same base as `Dockerfile.inference`, plus dev-only deps
  (xgboost, lightgbm, scikit-learn, optuna for tuning).
- `requirements.train.txt` — pinned versions.

### 2. Training script
- `scripts/train_and_register.py` — wraps the existing
  `scripts/train_pipeline.py` and adds:
  - Pull `training_table.csv` and `training_table_club.csv` from S3 (today's snapshot).
  - Generate a version timestamp (`v20260601-153012`).
  - Upload `model_final_*.pkl` + `rho.json` + `manifest.json` to
    `s3://<data-bucket>/artefacts/v<ts>/`.
  - Call `sagemaker.create_model_package` with status
    `PendingManualApproval` (or `Approved` if auto-approve gate passes).

### 3. Validation gate
- `scripts/validate_candidate.py`:
  - Load the *Approved* model package from SageMaker (the current production
    model) and the candidate (just-trained) model.
  - Run both against the same holdout. Compare on the chosen primary metric
    (default: outcome accuracy; configurable).
  - If candidate ≥ current + ε (default ε = 1pp), set status to `Approved`.
    Otherwise leave as `PendingManualApproval` for human review.

### 4. Inference Lambda update
- Edit `src/inference/predict.py`:
  - Replace the hard-coded artefact prefix with a SageMaker resolver:
    ```python
    def _resolve_approved_package(group_name: str) -> dict:
        client = boto3.client("sagemaker", region_name=os.environ["AWS_REGION"])
        resp = client.list_model_packages(
            ModelPackageGroupName=group_name,
            ModelApprovalStatus="Approved",
            SortBy="CreationTime",
            SortOrder="Descending",
            MaxResults=1,
        )
        pkg = client.describe_model_package(
            ModelPackageName=resp["ModelPackageSummaryList"][0]["ModelPackageArn"],
        )
        return pkg
    ```
  - Cache the resolved package ARN at module load (Lambda warm-window reuse).
- Add the `sagemaker:ListModelPackages` and `sagemaker:DescribeModelPackage`
  permissions to the inference Lambda's role.

### 5. GitHub Actions workflow
- `.github/workflows/train-and-register.yml` with:
  - `on: workflow_dispatch:` only (manual trigger, see "Triggers" below).
  - Inputs: `mode` (`national | club | both`), `auto_approve` (`true | false`).
  - Steps: AWS OIDC assume-role → docker build → docker run → python
    `scripts/train_and_register.py` → if auto_approve, run
    `scripts/validate_candidate.py`.

### 6. AWS infrastructure (CDK additions)
- New stack `FPMLOpsStack`:
  - Two `aws_sagemaker.CfnModelPackageGroup`: `fp-national-poisson`,
    `fp-club-poisson`.
  - GitHub OIDC provider + IAM role assumable from the repo, scoped to:
    - `s3:PutObject` on `artefacts/*`
    - `sagemaker:CreateModelPackage`, `UpdateModelPackage`,
      `ListModelPackages`, `DescribeModelPackage` on the two groups
  - SNS topic for "model awaiting approval" notifications + email subscription.

### 7. Promotion paths (manual fallback)
- Console: Model Registry → pick package version → "Approve" button.
- CLI: `aws sagemaker update-model-package --model-package-arn <arn>
  --model-approval-status Approved`.
- Both are idempotent; multi-engineer teams can sign off in either UI.

---

## Phases

| # | Phase | Output | Estimated effort |
|---|---|---|---|
| 1 | Container + training script + manifest | `Dockerfile.train`, `scripts/train_and_register.py`, S3 layout settled | 0.5 day |
| 2 | SageMaker Model Registry CDK stack | `FPMLOpsStack` deployed, two empty groups exist | 0.5 day |
| 3 | First end-to-end run from local machine | One model package version per mode, status `PendingManualApproval` | 0.25 day |
| 4 | Manually approve via console; redeploy inference Lambda to read from registry | Lambda runs against registry-resolved model | 0.5 day |
| 5 | GHA workflow + AWS OIDC trust | One-click training run from GitHub | 0.5 day |
| 6 | Validation gate + auto-approve option | `validate_candidate.py`, configurable threshold | 0.25 day |

Total: ~2.5 days of focused work, spread over a few sessions.

---

## Decisions to lock before implementing

1. **Primary validation metric.** Recommend outcome accuracy with ε = 1pp.
   Alternative: ranked probability score (RPS), more discriminating but
   harder to explain to non-ML stakeholders.
2. **Auto-approve in v0?** Recommend false (humans approve via console for
   the first ~10 versions; flip to auto when you trust the gate).
3. **Triggers.** Recommend `workflow_dispatch` only for v0. Add `schedule`
   (e.g., weekly Monday 06:00 UTC) once two consecutive manual runs have
   succeeded cleanly.
4. **Artefact retention.** Recommend keep last 20 versions in S3, expire
   older with a lifecycle rule. Approved versions never expire.
5. **Multi-engineer approval gate?** Defer. SageMaker supports it but it's
   over-engineered for solo + early team. Single approver is fine.

---

## What this plan deliberately defers

- **SageMaker Pipelines** — the orchestration layer. Skipped because GHA does
  it well enough at this scale. Revisit when training graduates to GPU/Spot
  or hyperparameter sweeps.
- **SageMaker Model Monitor** — drift detection. Skipped until Phase 8 (live
  WC 2026 tracking) when there's enough resolved-fixture volume to make
  drift signals statistically meaningful.
- **A/B testing / shadow deploys** — useful for online learning, not needed
  for daily-batch inference.
- **Custom container for inference** — current Lambda image works fine;
  SageMaker endpoints are not required.

---

## Open questions

- **Where do validation holdouts live?** Currently the holdout is computed
  inside `_make_holdout_masks` from `train.py`. To validate in a fresh
  process (the GHA runner), we either re-compute the same way (current code
  already does this) or persist a frozen holdout snapshot to S3. Recommend
  the latter once we have multiple modellers — guarantees reproducibility
  across runs.
- **Code SHA in the manifest** — easy to capture in GHA via
  `${{ github.sha }}`, harder for local runs. Should the manifest fall back
  to `git rev-parse HEAD` when run locally? (Yes.)
- **Cross-account move later?** If MLOps engineers want a separate AWS
  account for the model registry (SOC2 / blast-radius reasons), the OIDC
  role and Model Package Group ARNs need to move. Not v0 work, but the
  plan should be portable.
