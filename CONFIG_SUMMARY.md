# 📝 NYC Taxi Pipeline - Configuration Summary

## ✅ Created/Updated Files

### 1. Bundle Configuration
**File:** `databricks.yml`
- Bundle name: `nyc-taxi`
- 3 targets: dev (default), staging, prod
- Variables: catalog, schema, warehouse_id, pipeline_development
- Auto-includes all resource files in `resources/*.yml`

### 2. Resource Files

#### 📊 Pipeline Resource
**File:** `resources/nyc_taxi_pipeline.pipeline.yml`
- **Name:** NYC Taxi Data Pipeline - ${bundle.target}
- **Type:** Spark Declarative Pipeline (SDP)
- **Compute:** Serverless + Photon
- **Source:** Bronze, Silver, Gold layers from `src/pipeline/`
- **Mode:** Triggered (not continuous)
- **Notifications:** Email alerts on failures
- **Permissions:** 
  - CAN_VIEW: users group
  - CAN_RUN: data_engineers group

#### 🔄 Job Resource
**File:** `resources/nyc_taxi_job.job.yml`
- **Name:** NYC Taxi Daily Pipeline - ${bundle.target}
- **Schedule:** Daily at 2 AM (Asia/Bangkok)
- **Tasks:** Run nyc_taxi_pipeline
- **Timeout:** 2 hours per run
- **Notifications:** Email on success/failure
- **Permissions:** 
  - CAN_VIEW: users group
  - CAN_MANAGE_RUN: data_engineers group

#### 💾 Volume Resource
**File:** `resources/nyc_taxi_volume.volume.yml`
- **Name:** nyc_taxi_data
- **Type:** Managed Volume
- **Purpose:** Store raw data and intermediate files
- **Grants:**
  - READ_VOLUME, WRITE_VOLUME: data_engineers
  - READ_VOLUME: users

### 3. CI/CD Workflow
**File:** `.github/workflows/ci-cd.yml`
- **6 Jobs:** test → lint → validate → deploy-dev/staging/prod
- **Triggers:** Push and PR to main, dev, staging branches
- **Features:**
  - Unit & integration tests with pytest
  - Code quality checks (Black, Flake8, isort, mypy)
  - Bundle validation for each environment
  - Auto-deploy based on branch
  - Coverage reporting to Codecov

---

## 🎯 Environment Configuration

### Development (dev)
```yaml
catalog: dev_catalog
schema: nyc_taxi_dev
pipeline_development: true
mode: development
```

### Staging
```yaml
catalog: staging_catalog
schema: nyc_taxi_staging
pipeline_development: true
mode: development
```

### Production (prod)
```yaml
catalog: prod_catalog
schema: nyc_taxi_prod
pipeline_development: false
mode: production
```

---

## 🚀 Quick Start Commands

### Local Testing
```bash
# 1. Validate configuration
databricks bundle validate --target dev

# 2. Deploy to development
databricks bundle deploy --target dev

# 3. Run pipeline
databricks bundle run nyc_taxi_pipeline --target dev
```

### CI/CD Flow
```bash
# Development flow
git checkout -b feature/my-feature
git add .
git commit -m "Add new feature"
git push origin feature/my-feature
# Create PR to dev → tests run automatically

# Merge to dev → auto deploy to development

# Merge dev to staging → auto deploy to staging

# Merge staging to main → auto deploy to production (with approval)
```

---

## 📦 Resource Dependencies

```
databricks.yml
    ├── resources/nyc_taxi_pipeline.pipeline.yml
    │       └── Source code: src/pipeline/**/*.py
    │           ├── bronze/bronze_layer.py
    │           ├── silver/silver_layer.py
    │           └── gold/gold_layer.py
    │
    ├── resources/nyc_taxi_job.job.yml
    │       └── References: ${resources.pipelines.nyc_taxi_pipeline.id}
    │
    └── resources/nyc_taxi_volume.volume.yml
            └── Storage for raw data
```

---

## 🔧 Required GitHub Secrets

Set these in: **Repository → Settings → Secrets → Actions**

| Secret | Description | Example |
|--------|-------------|---------|
| `DATABRICKS_HOST` | Workspace URL | `https://dbc-xxxx.cloud.databricks.com` |
| `DATABRICKS_DEV_TOKEN` | Dev token | dapi... (PAT or SP token) |
| `DATABRICKS_STAGING_TOKEN` | Staging token | dapi... |
| `DATABRICKS_PROD_TOKEN` | Production token | dapi... |

---

## ⚙️ Configuration Variables

### Bundle-level Variables
Defined in `databricks.yml`, can be overridden per target:

```yaml
variables:
  catalog:                    # Unity Catalog name
    description: Unity Catalog name
    default: main
  
  schema:                     # Target schema
    description: Target schema for pipeline
    default: default
  
  warehouse_id:               # SQL Warehouse ID (if needed)
    description: SQL Warehouse ID for jobs
    default: ""
  
  pipeline_development:       # Development mode
    description: Enable development mode for pipeline
    default: true
```

### Usage in Resources
Variables are referenced using `${var.variable_name}`:
- `${var.catalog}` → Catalog name for target
- `${var.schema}` → Schema name for target
- `${var.pipeline_development}` → true/false for dev mode

### Cross-resource References
Resources can reference each other:
- `${resources.pipelines.nyc_taxi_pipeline.id}` → Pipeline ID
- `${bundle.target}` → Current target name (dev/staging/prod)

---

## 📋 Validation Checklist

Before deploying:

- [ ] All tests passing: `pytest tests/ -v`
- [ ] Code formatted: `black src/ tests/`
- [ ] Bundle valid: `databricks bundle validate --target dev`
- [ ] Catalogs exist in target workspace
- [ ] GitHub secrets configured
- [ ] Service principal has required permissions (if using)
- [ ] Email addresses updated in notification configs

---

## 🎨 Customization Points

### Pipeline Configuration
- **Schedule:** Modify cron expression in `nyc_taxi_job.job.yml`
- **Notifications:** Update email addresses
- **Compute:** Change from serverless to classic clusters if needed
- **Mode:** Change from triggered to continuous

### CI/CD Workflow
- **Branches:** Add/remove branches in `.github/workflows/ci-cd.yml`
- **Tests:** Add test commands in test job
- **Approvals:** Set up GitHub Environments for approvals

### Catalogs & Schemas
- **Names:** Update catalog/schema names in target variables
- **Permissions:** Modify permission grants in resource files

---

## 📚 Reference Documentation

- **Full Deployment Guide:** [DEPLOYMENT.md](DEPLOYMENT.md)
- **Project README:** [README.md](README.md)
- **Databricks Bundles:** https://docs.databricks.com/dev-tools/bundles/
- **SDP Pipelines:** https://docs.databricks.com/workflows/sdp/
- **GitHub Actions:** https://docs.github.com/actions

---

## 🆘 Quick Troubleshooting

### Validation fails
```bash
# Check bundle syntax
databricks bundle validate --target dev

# Check file paths
ls -la resources/
ls -la src/pipeline/
```

### Deploy fails
```bash
# Check authentication
databricks auth profiles

# Check permissions
databricks catalogs list
databricks schemas list --catalog-name dev_catalog
```

### CI/CD fails
```bash
# Check GitHub Actions logs
# Go to: Repository → Actions → Click failed run

# Common issues:
# 1. Missing secrets
# 2. Expired tokens
# 3. Service principal permissions
```

---

**Last Updated:** 2024-01-17
**Configuration Version:** 1.0
**Bundle Name:** nyc-taxi
