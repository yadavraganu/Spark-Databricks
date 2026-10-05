# Databricks Asset Bundles — Deep Dive Guide

> **Note on naming:** Databricks renamed this feature from **Databricks Asset Bundles (DABs)** to **Declarative Automation Bundles**. The rename is backward-compatible — all existing CLI commands, YAML schema, and the `DAB` abbreviation still work exactly as before. This guide uses "Asset Bundles / DABs" since that's still the common usage, but you'll see the new name in current docs.

## 1. What Are Asset Bundles?

Databricks Asset Bundles are an **infrastructure-as-code (IaC)** approach to managing Databricks projects. Instead of clicking through the UI to create jobs, pipelines, and other resources, you define everything as version-controlled YAML (or Python) and deploy it with a single CLI command.

**Core idea:** if a resource exists in Databricks, it should exist as a file in your repo.

What you get:
- Jobs, DLT/Lakeflow pipelines, ML models, experiments, model-serving endpoints, schemas, catalogs, dashboards, clusters, apps, vector search indexes, SQL alerts, and (newer) Lakebase Postgres instances — all definable as code.
- A single `databricks bundle deploy` command to push everything to a workspace.
- Built-in support for multiple environments (dev/staging/prod) from one codebase.
- Native fit for Git-based CI/CD (GitHub Actions, Azure DevOps, GitLab CI).

**Prerequisite:** The Databricks CLI, installed and authenticated against your workspace. Check it's working with `databricks -v`. Newer CLI releases default to the faster direct deployment engine automatically (see §7) — if a feature in this guide isn't available, updating the CLI is usually the fix.

## 2. Architecture at a Glance

```
my-project/
├── databricks.yml          # Entry point — exactly one per bundle
├── resources/
│   ├── jobs/*.yml           # Workflow/job definitions
│   ├── pipelines/*.yml      # DLT / Lakeflow pipeline definitions
│   ├── schemas/*.yml        # Catalog/schema definitions
│   ├── my_app.app.yml       # Databricks Apps definitions
│   └── ...                  # dashboards, models, clusters, etc.
├── src/
│   └── ...                  # Notebooks, Python packages, app source
└── tests/
```

- **`databricks.yml`** must exist exactly once at the project root and is the entry point.
- Other files are pulled in via an `include:` glob (commonly `resources/*.yml`).
- Everything is parsed, validated, and deployed through the CLI — no manual UI steps required.

## 3. The `databricks.yml` File

A minimal bundle:

```yaml
bundle:
  name: my-project

include:
  - resources/*.yml

targets:
  dev:
    default: true
    workspace:
      host: https://your-workspace.cloud.databricks.com
  prod:
    workspace:
      host: https://your-prod-workspace.cloud.databricks.com
```

A more complete example showing the main top-level keys:

```yaml
bundle:
  name: my-project
  uuid: auto   # generated automatically

variables:
  warehouse_id:
    description: SQL warehouse to use for jobs
    default: abc123

include:
  - resources/*.yml

artifacts:
  default:
    type: whl
    build: python setup.py bdist_wheel
    path: .

resources:
  jobs:
    hello_job:
      name: hello-job
      tasks:
        - task_key: main
          notebook_task:
            notebook_path: ./src/notebook.py

targets:
  dev:
    default: true
    mode: development
    workspace:
      host: https://dev-workspace.cloud.databricks.com
    run_as:
      user_name: me@company.com

  prod:
    mode: production
    workspace:
      host: https://prod-workspace.cloud.databricks.com
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
    run_as:
      service_principal_name: prod-sp
    permissions:
      - level: CAN_MANAGE
        group_name: data-engineers
```

### Top-level keys
| Key | Purpose |
|---|---|
| `bundle` | Name, UUID, and bundle-level settings |
| `include` | Glob patterns pulling in other YAML files |
| `variables` | Reusable, overridable parameters |
| `artifacts` | Build instructions for wheels/jars before deployment |
| `resources` | Inline resource definitions (jobs, pipelines, etc.) |
| `targets` | Named deployment environments |
| `sync` | Controls which local files get synced to the workspace |
| `permissions` | Access control applied to deployed resources |

## 4. Resources

Resources are the actual Databricks objects the bundle creates/manages. Common resource types:

- `resources.jobs` — Workflow jobs (notebook, Python, SQL, dbt tasks, etc.)
- `resources.pipelines` — DLT / Lakeflow Declarative Pipelines
- `resources.experiments` / `resources.models` / `resources.registered_models` — MLflow objects
- `resources.model_serving_endpoints`
- `resources.schemas` / catalogs — Unity Catalog objects
- `resources.clusters`
- `resources.dashboards` — supports `dataset_catalog` / `dataset_schema` params
- `resources.apps` — Databricks Apps (see note below)
- `resources.vector_search_indexes` / endpoints
- `resources.sql_alerts` (newer addition)

Example job definition (`resources/jobs/ingestion.yml`):

```yaml
resources:
  jobs:
    ingestion_job:
      name: daily-ingestion
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: UTC
      tasks:
        - task_key: load
          notebook_task:
            notebook_path: ../src/ingest.py
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: i3.xlarge
            num_workers: 2
```

### Apps are different
Databricks Apps resources behave differently from other resource types:
- Environment variables go in **`app.yaml`** inside the app's source directory — **not** in `databricks.yml`.
- `source_code_path` is relative to the `resources/` directory (so typically `../src/app`), while paths referenced directly inside `databricks.yml` itself are relative to the project root (`./src/...`).
- This path-resolution mismatch (`../src/` in `resources/*.yml` vs. `./src/` in `databricks.yml`) is one of the most common sources of "path not found" errors — keep it in mind when debugging.

Use `databricks bundle schema` to inspect the full, current schema for any resource type.

## 5. Targets (Environments)

Targets let one bundle deploy differently per environment:

```yaml
targets:
  dev:
    default: true
    mode: development     # enables dev-only conveniences (prefixing, etc.)
    workspace:
      host: https://dev-workspace.cloud.databricks.com

  staging:
    workspace:
      host: https://staging-workspace.cloud.databricks.com

  prod:
    mode: production       # stricter validation, no dev prefixing
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    run_as:
      service_principal_name: prod-deployer-sp
```

- `mode: development` auto-prefixes resource names with your username and tags them, so multiple developers can deploy to a shared dev workspace without collisions.
- `mode: production` enforces stricter checks (e.g., requiring an explicit `run_as`).
- Resource definitions can be overridden per-target inside the `targets` block (e.g., smaller clusters in dev, autoscaling in prod).

## 6. Variables

Variables make bundles reusable across environments:

```yaml
variables:
  catalog:
    description: Unity Catalog catalog to use
    default: dev_catalog

resources:
  jobs:
    my_job:
      tasks:
        - task_key: main
          notebook_task:
            notebook_path: ./src/notebook.py
            base_parameters:
              catalog: ${var.catalog}

targets:
  prod:
    variables:
      catalog: prod_catalog
```

Variables can also be overridden at deploy time:
```bash
databricks bundle deploy -t prod --var="catalog=prod_catalog"
```

## 7. Deployment Engine: Direct vs. Terraform

Historically, DABs deployed resources through an embedded Terraform provider, with state tracked in `terraform.tfstate`. Databricks has since introduced a **direct deployment engine** that talks to the Databricks REST APIs directly (via the Go SDK), and newer CLI releases use it by default:

- No need to download Terraform / the `terraform-provider-databricks` binary before deploying.
- Faster deployments, no Terraform state-drift issues.
- Uses its own JSON state file (`resources.json`) instead of `terraform.tfstate`.
- Some behavior differs subtly: with Terraform, removing a field from `databricks.yml` leaves the live value unchanged; with the direct engine, removing a field reverts it to the resource's default.
- Databricks plans to deprecate the Terraform engine entirely, so migrating is the recommended path even for bundles that started out on Terraform.
- Some resource types are **direct-engine-only** and cannot be deployed under Terraform at all: `genie_spaces`, `catalogs`, `external_locations`, and Vector Search endpoints/indexes.

**Enabling it** — the correct key is `bundle.engine` (not nested under a `deployment:` block):

```yaml
bundle:
  name: my-project
  engine: direct    # or "terraform" for the legacy behavior
```

It can also be set **per target**, or via an environment variable:

```yaml
targets:
  dev:
    engine: direct   # direct engine just for dev, e.g. while migrating gradually
```

```bash
DATABRICKS_BUNDLE_ENGINE=direct databricks bundle deploy -t my_target
```

**Migrating an existing Terraform-backed bundle** (recommended one target at a time, lowest-risk environment first):

```bash
# 1. Add engine: direct to databricks.yml for the target you're migrating
# 2. Convert state: reads terraform.tfstate, writes resources.json
databricks bundle deployment migrate --target dev

# 3. Preview what would change under the new engine (like `terraform plan`)
databricks bundle plan --target dev

# 4. Deploy as normal
databricks bundle deploy --target dev
```

This is one of the more substantive parts of the Asset Bundles → Declarative Automation Bundles rename — it's not purely cosmetic.

## 8. Core CLI Workflow

```bash
# Scaffold a new bundle from a template
databricks bundle init

# Reverse-engineer an existing job/pipeline into bundle YAML
databricks bundle generate job --existing-job-id <job-id>
databricks bundle generate pipeline --existing-pipeline-id <pipeline-id>

# Parse + validate config (schema check only — no workspace calls)
databricks bundle validate -t dev

# Preview what would change on the next deploy (direct engine only)
databricks bundle plan -t dev

# Deploy resources to the target workspace
databricks bundle deploy -t dev

# Run a specific resource (e.g., a job)
databricks bundle run ingestion_job -t dev

# Tear down everything the bundle deployed to a target
databricks bundle destroy -t dev
```

`databricks bundle generate` is a handy on-ramp for brownfield adoption: point it at an existing job or pipeline ID and it writes the matching YAML (and, for pipelines, pulls down the associated notebook source) so you can bring something already running in the UI under bundle management instead of writing it by hand.

Key point: `databricks bundle validate` **only checks syntax/schema and resolves variables** — it does not contact the workspace to verify resources exist or permissions are correct. Real validation happens at `deploy` time.

## 9. Python-Defined Bundles

Bundles can be defined in **Python** instead of (or alongside) YAML — useful for teams that want programmatic resource generation, loops, or conditional logic rather than static YAML.

```python
from databricks.bundles.jobs import Job

def load_resources(bundle):
    return {
        "jobs": {
            "my_job": Job(
                name="my-python-defined-job",
                tasks=[...],
            )
        }
    }
```

This is additive — most teams still use YAML, reaching for Python bundles only when config needs real logic (e.g., generating N similar jobs from a list).

## 10. Bundles in the Workspace

You no longer need the CLI locally to work with bundles. **Declarative Automation Bundles in the Workspace** lets you:

- Open a Git folder inside the Databricks workspace UI.
- Create a bundle from a pre-built template directly in-browser.
- Import existing jobs/pipelines into a bundle.
- Collaborate, version, and deploy — all without leaving the workspace.

This is aimed at teams/analysts who want IaC discipline without needing a local dev environment.

## 11. CI/CD Integration

Typical pattern with GitHub Actions:

```yaml
name: Deploy Bundle
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
```

Best practices:
- Use a **service principal**, not a personal token, for prod deployments (`run_as: service_principal_name`).
- Run `databricks bundle validate` as a PR check before merge.
- Gate `deploy -t prod` behind branch protection / manual approval.
- Keep separate OAuth/service-principal credentials per target environment.

## 12. Best Practices Checklist

- ✅ One bundle per logical project; don't cram unrelated pipelines into one `databricks.yml`.
- ✅ Use `include: - resources/*.yml` and split resources into logical files (`jobs/`, `pipelines/`, `schemas/`) rather than one giant file.
- ✅ Use `mode: development` in dev targets to avoid name collisions between developers.
- ✅ Use `mode: production` + `run_as: service_principal_name` for prod — never deploy prod as a personal user.
- ✅ Parameterize environment-specific values (catalog names, cluster sizes, warehouse IDs) with `variables`, not hardcoded YAML per target.
- ✅ Remember the Apps path-resolution quirk (`../src/` in `resources/*.yml` vs `./src/` in `databricks.yml`) and that App env vars live in `app.yaml`, not `databricks.yml`.
- ✅ Run `databricks bundle validate` (and ideally `bundle plan`) in CI before every `deploy`.
- ✅ If still on the Terraform engine, plan a migration to `direct` — it's becoming the default and some newer resource types (`genie_spaces`, `catalogs`, `external_locations`, Vector Search) only work under it. Migrate one target at a time, lowest-risk first.
- ✅ Pin a specific CLI version in CI so everyone deploys with the same schema and engine behavior, rather than letting it float.
- ✅ Use `databricks bundle schema` locally when unsure of a resource's exact fields — it reflects your installed CLI version, which is more reliable than memorized syntax.

## 13. Quick Reference

| Command | Purpose |
|---|---|
| `databricks bundle init` | Scaffold a new bundle from a template |
| `databricks bundle generate job/pipeline --existing-*-id <id>` | Convert an existing resource into bundle YAML |
| `databricks bundle validate -t <target>` | Schema/syntax check, resolves variables |
| `databricks bundle plan -t <target>` | Preview changes before deploying (direct engine) |
| `databricks bundle deploy -t <target>` | Deploy resources to workspace |
| `databricks bundle deployment migrate -t <target>` | Convert Terraform state → direct-engine state |
| `databricks bundle run <resource_key> -t <target>` | Trigger a job/pipeline run |
| `databricks bundle destroy -t <target>` | Remove all deployed resources for that target |
| `databricks bundle schema` | Print the current CLI's full resource schema |
| `databricks -v` | Check installed CLI version |

---

*Sources: Databricks official documentation (docs.databricks.com/dev-tools/bundles), Declarative Automation Bundles release notes, and current community/MVP coverage of the rename and the direct deployment engine.*
