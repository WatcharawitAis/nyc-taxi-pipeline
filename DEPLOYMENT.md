# 🚀 NYC Taxi Pipeline - Deployment Guide

## 📋 Table of Contents
1. [Prerequisites](#prerequisites)
2. [Configuration Files](#configuration-files)
3. [GitHub Secrets Setup](#github-secrets-setup)
4. [Local Deployment](#local-deployment)
5. [CI/CD Pipeline](#cicd-pipeline)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Tools
- **Databricks CLI** (v0.200.0+)
  ```bash
  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
  ```
- **Python 3.10+** (for local testing)
- **Git** (for version control)

### Databricks Requirements
- Workspace with Unity Catalog enabled
- Service Principal (recommended for production)
- Catalogs created: `dev_catalog`, `staging_catalog`, `prod_catalog`
- Schemas will be created automatically by the bundle

---

## Configuration Files

### 1. databricks.yml (Bundle Configuration)

```yaml
bundle:
  name: nyc-taxi

variables:
  catalog: main
  schema: default
  pipeline_development: true

targets:
  dev:       # Development environment
  staging:   # Staging environment
  prod:      # Production environment
```

**Key Variables:**
- `catalog`: Unity Catalog name for each environment
- `schema`: Target schema for pipeline tables
- `pipeline_development`: Enable development mode (true for dev/staging)

### 2. Resource Files

#### Pipeline Resource (`resources/nyc_taxi_pipeline.pipeline.yml`)
Defines the Spark Declarative Pipeline with:
- Bronze, Silver, Gold transformations
- Serverless compute with Photon
- Email notifications on failures
- Permissions for users and data engineers

#### Job Resource (`resources/nyc_taxi_job.job.yml`)
Defines scheduled job to run the pipeline:
- Daily at 2 AM (Asia/Bangkok timezone)
- Email notifications on success/failure
- 2-hour timeout per run
- Reference to pipeline via `${resources.pipelines.nyc_taxi_pipeline.id}`

#### Volume Resource (`resources/nyc_taxi_volume.volume.yml`)
Managed volume for raw data storage:
- Created in target catalog/schema
- Read/Write access for data engineers
- Read-only access for general users

---

## GitHub Secrets Setup

### Required Secrets

ตั้งค่า secrets ใน GitHub repository → Settings → Secrets and variables → Actions

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `DATABRICKS_HOST` | Workspace URL | `https://dbc-xxxx.cloud.databricks.com` |
| `DATABRICKS_DEV_TOKEN` | Dev environment token | Personal Access Token or SP token |
| `DATABRICKS_STAGING_TOKEN` | Staging environment token | Personal Access Token or SP token |
| `DATABRICKS_PROD_TOKEN` | Production environment token | Personal Access Token or SP token |

### Creating Databricks Tokens

**Option 1: Personal Access Token (for testing)**
```bash
1. Workspace → User Settings → Developer → Access Tokens
2. Click "Generate New Token"
3. Set expiration (recommend 90 days)
4. Copy token immediately (shown only once)
```

**Option 2: Service Principal (recommended for production)**
```bash
# Create service principal
databricks service-principals create --display-name "nyc-taxi-pipeline-sp"

# Create token for service principal
databricks tokens create --lifetime-seconds 31536000 \
  --comment "NYC Taxi Pipeline CI/CD"
```

---

## Local Deployment

### Step 1: Authenticate CLI

```bash
# Configure default profile
databricks configure --host https://your-workspace.cloud.databricks.com

# Or use environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token-here"
```

### Step 2: Validate Bundle

```bash
# Validate configuration
databricks bundle validate --target dev

# Should output: "Validation OK!"
```

### Step 3: Deploy to Development

```bash
# Deploy bundle (creates/updates all resources)
databricks bundle deploy --target dev

# View deployment summary
databricks bundle summary --target dev
```

### Step 4: Run Pipeline

```bash
# Run pipeline manually
databricks bundle run nyc_taxi_pipeline --target dev

# Or run the scheduled job
databricks bundle run nyc_taxi_daily_job --target dev
```

### Step 5: Monitor Pipeline

```bash
# View pipeline updates
databricks pipelines list-updates \
  --pipeline-id $(databricks bundle summary --target dev | grep nyc_taxi_pipeline | awk '{print $2}')
```

---

## CI/CD Pipeline

### Workflow Overview

```
┌─────────────────────────────────────────────────────────┐
│  1. Push/PR to branch                                   │
└────────────────┬────────────────────────────────────────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
    ▼            ▼            ▼
┌───────┐  ┌─────────┐  ┌──────────┐
│ Tests │  │  Lint   │  │ Validate │
└───┬───┘  └────┬────┘  └────┬─────┘
    │           │            │
    └───────────┴────────────┘
                 │
                 ▼
        ┌────────────────┐
        │  Deploy based  │
        │  on branch     │
        └────────────────┘
```

### Branch Strategy

| Branch | Auto Deploy To | Approval Required |
|--------|---------------|-------------------|
| `dev` | Development | No |
| `staging` | Staging | No |
| `main` | Production | Yes (GitHub Environment) |

### Workflow Jobs

1. **test** - Run unit and integration tests
2. **lint** - Code quality checks (Black, Flake8, isort, mypy)
3. **validate** - Validate Databricks bundle
4. **deploy-dev** - Deploy to development (auto on `dev` branch push)
5. **deploy-staging** - Deploy to staging (auto on `staging` branch push)
6. **deploy-prod** - Deploy to production (auto on `main` branch push)

### Setting Up GitHub Environments

**For Production Approvals:**

```bash
1. Go to repository → Settings → Environments
2. Create "production" environment
3. Add required reviewers (e.g., team leads)
4. Optional: Add deployment branches rule (only `main`)
```

---

## Deployment Commands Reference

```bash
# Validate before deploy
databricks bundle validate --target <env>

# Deploy to specific environment
databricks bundle deploy --target dev
databricks bundle deploy --target staging
databricks bundle deploy --target prod

# Force deployment (override locks)
databricks bundle deploy --target dev --force-lock

# Run resources
databricks bundle run nyc_taxi_pipeline --target dev
databricks bundle run nyc_taxi_daily_job --target dev

# View deployed resources
databricks bundle summary --target dev

# Destroy all resources (be careful!)
databricks bundle destroy --target dev --auto-approve
```

---

## Troubleshooting

### Common Issues

#### 1. Bundle Validation Failed

**Error:** `cannot resolve bundle auth configuration`
```bash
# Solution: Check CLI configuration
databricks auth profiles
databricks configure --profile default
```

**Error:** `expected a notebook but got a file`
```bash
# Solution: Verify file paths in pipeline.yml
# Files should be .py or .ipynb in src/pipeline/
ls -la src/pipeline/**/*.py
```

#### 2. Deployment Failed

**Error:** `catalog not found`
```bash
# Solution: Create catalog manually
databricks catalogs create --name dev_catalog
databricks schemas create --name nyc_taxi_dev --catalog-name dev_catalog
```

**Error:** `insufficient permissions`
```bash
# Solution: Grant permissions to service principal
databricks catalogs grant --catalog dev_catalog \
  --principal <service-principal-name> \
  --privileges USE_CATALOG,CREATE_SCHEMA
```

#### 3. Pipeline Execution Failed

**Check pipeline logs:**
```bash
# Get pipeline ID
PIPELINE_ID=$(databricks bundle summary --target dev | grep nyc_taxi_pipeline | awk '{print $2}')

# List updates
databricks pipelines list-updates --pipeline-id $PIPELINE_ID

# Get latest update details
databricks pipelines get-update \
  --pipeline-id $PIPELINE_ID \
  --update-id <update-id>
```

#### 4. CI/CD Workflow Failed

**Check GitHub Actions logs:**
```bash
1. Go to repository → Actions tab
2. Click on failed workflow run
3. Expand failed job to see error details
```

**Common fixes:**
- Verify GitHub secrets are set correctly
- Check token expiration
- Ensure service principal has required permissions
- Validate bundle locally before pushing

---

## Best Practices

### Security
- ✅ Use service principals for production deployments
- ✅ Rotate tokens regularly (every 90 days)
- ✅ Use GitHub Environment secrets for production
- ✅ Never commit tokens to Git

### Development Workflow
- ✅ Test locally before pushing: `databricks bundle validate`
- ✅ Use feature branches for new development
- ✅ Run tests locally: `pytest tests/ -v`
- ✅ Code formatting: `black src/ tests/`

### Deployment
- ✅ Deploy to dev → staging → prod progression
- ✅ Validate each environment after deployment
- ✅ Monitor pipeline runs for data quality issues
- ✅ Keep deployment logs for audit trail

---

## Support

**Documentation:**
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Spark Declarative Pipelines](https://docs.databricks.com/workflows/sdp/)
- [GitHub Actions with Databricks](https://docs.databricks.com/dev-tools/ci-cd/ci-cd-github.html)

**Contact:**
- Data Engineering Team: watchaaq@ais.co.th
- Slack: #data-engineering
