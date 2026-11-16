## 1. The Big Picture

**Pricing tier:** CMK, inter-node encryption, and SQL query encryption are all **Enterprise tier** features. CMK for Unity Catalog also requires Enterprise.

**Memory hook:** *Control plane = managed services key. Your bucket + disks = workspace storage key. Per-catalog = UC CMK. Your own S3 data = SSE-KMS via external location. Wire between workers = init script. Logs = redaction.*

## 2. Customer-Managed Keys (CMK): The Two Core Use Cases

A CMK is a key in **your** AWS KMS that Databricks uses to protect specific data. Two use cases, each independently configurable:

### 2.1 Managed Services Key (Control Plane Data)

Data stored in the Databricks control plane, encrypted at rest. A CMK protects:

- Notebook source and metadata
- Interactive notebook results stored in the control plane (not jobs)
- Secrets (Databricks secrets / secret manager APIs)
- AI/BI dashboards
- Genie Agents
- Databricks SQL queries and query history
- PATs / credentials used for Git integration (Git folders)
- AI Search indexes and metadata (serverless compute plane)
- Lakebase Autoscaling project data at rest
- Model Serving container images and model artifacts in the Databricks-managed registry
- **Serverless workspaces only:** also covers workspace storage / root storage (job results, SQL results, notebook revisions, default storage)

### 2.2 Workspace Storage Key (Your AWS Account)

- **Workspace storage bucket (S3):** contains **DBFS root** (FileStore, MLflow Models, Lakeflow pipeline data on DBFS root) plus workspace system data (job results, Databricks SQL results, notebook revisions).
- **EBS volumes (optional):** remote EBS volumes of cluster nodes in the **classic compute plane**.
- **Does NOT encrypt:** additional DBFS mounts (mounts are a legacy pattern; use Unity Catalog instead), and does not apply to default storage.

### 2.3 Which key protects what

| Data | Classic workspace | Serverless workspace |
| --- | --- | --- |
| Notebook source, secrets, SQL queries/history, dashboards, Git creds | Managed services | Managed services |
| DBFS root / FileStore | Workspace storage | Managed services (default storage) |
| Job results | Workspace storage | Managed services |
| Databricks SQL query results | Workspace storage | Managed services |
| MLflow Models | Workspace storage | Managed services |
| Lakeflow pipelines (DBFS root path) | Workspace storage | Managed services |
| Interactive notebook results | Split: partial results in control plane → managed services key; results in your bucket (if configured to store all there) → workspace storage key | Managed services |
| Cluster EBS volumes | Workspace storage | **Not applicable** (ephemeral) |
| Model Serving images/artifacts (Databricks-created) | Managed services | Managed services |
| Model Serving artifacts you create (root storage; serverless column inferred from the serverless rule) | Workspace storage | Managed services |

### 2.4 Serverless specifics

- Serverless workspaces: **configure only the managed services key** (default storage is treated as a managed service).
- EBS encryption doesn't apply to serverless compute: disks are short-lived and destroyed when the workload stops or scales down.
- Databricks SQL Serverless and serverless compute support both managed-services keys and workspace storage keys (including root storage for SQL and notebook results).

### 2.5 Gotcha: query history system table

After enabling a managed services CMK, fields like `statement_text` in the **query history system table** appear **empty** until an account admin also configures a CMK in the `system` catalog's data encryption settings.

## 3. Configuring CMK (Step by Step)

### 3.1 Concept: the encryption keys configuration

- An **encryption keys configuration** is an **account-level object** that references your cloud key.
- Created by **account admins** in the account console (Security → Encryption keys) or via the **Account Key Configurations API**.
- Can be attached to **one or more workspaces**.
- **One configuration can serve both use cases**, and the same AWS KMS key can be used for both.
- Check region support; the KMS key must be in the **same AWS region as the workspace**.

### 3.2 Step 1: Create or select a KMS key

- Must be a **symmetric** key.
- Copy the **Key ARN** and **Key alias**.
- **Add** (don't replace) statements to the key policy. The principal is the Databricks AWS account **`414351767826`** (GovCloud: `044793339203`; GovCloud DoD: `170661010020`).
- Policy variants (choose per use case):
  - **Managed services:** allows `kms:Encrypt`, `kms:Decrypt` to Databricks root principal.
  - **Workspace storage (DBFS):** allows `kms:Encrypt`, `kms:Decrypt`, `kms:ReEncrypt*`, `kms:GenerateDataKey*`, `kms:DescribeKey`.
  - **Workspace storage (EBS):** principal is **your cross-account IAM role ARN** (the same one used for the Databricks credential configuration); allows `kms:Decrypt`, `kms:GenerateDataKey*`, `kms:CreateGrant`, `kms:DescribeKey` with condition `kms:ViaService` like `ec2.*.amazonaws.com`.
- Root-principal statements carry a condition on the tag **`aws:PrincipalTag/DatabricksAccountId`** matching your Databricks account ID. This scopes use of your key to *your* Databricks account.

### 3.3 Step 2 (optional): Cross-account IAM role policy

Required **only if the KMS key is in a different AWS account** than the cross-account IAM role used to deploy the workspace. Add an inline policy on that role allowing `kms:Decrypt`, `kms:GenerateDataKey*`, `kms:CreateGrant`, `kms:DescribeKey` on the key ARN, with `kms:ViaService` = `ec2.*.amazonaws.com`.

### 3.4 Step 3: Create the key configuration

Account console → **Security** → **Encryption keys** → **Add encryption key**. Pick use case (**Both**, **Managed services**, or **Workspace storage**), enter **AWS key ARN** and **alias**, click Add, and **copy the configuration Id**.

### 3.5 Step 4: Attach to a workspace

- **New workspace:** specify the key configuration during creation (manual AWS config or Account API).
- **Existing workspace:** the **account console does not support adding workspace storage keys** to existing workspaces. Use the **Databricks CLI** or the **Update workspace configuration API**.
- CLI flow: (1) **terminate all running compute**, (2) run `databricks account workspaces update <workspace-id>` setting `managed_services_customer_managed_key_id` and/or `storage_customer_managed_key_id`, (3) **restart compute**.
- **Workspace storage key updates can take up to 24 hours** to take effect.

### 3.6 Key rotation

| Key type | Can you rotate by swapping the key config? | Notes |
| --- | --- | --- |
| Managed services | **Yes** (repeat Step 4 with a new config) | Keep the **old KMS key available to Databricks for 24 hours** |
| Workspace storage | **No** | Use **AWS automatic key rotation**; it rotates underlying key material without changing the ARN and is compatible |

## 4. Encrypting Queries, Query History, and Query Results

| Object | Where stored | Key to use |
| --- | --- | --- |
| Databricks SQL **queries and query history** | Control plane | **Managed services** key |
| Databricks SQL **query results** | Your **workspace storage bucket** | **Workspace storage** key |

Key points:

- If a managed services CMK is already set, **no further action** is needed for queries/history.
- Protects **data at rest only**: not data in transit or in memory.
- Queries/history stored **before the key was added, or before May 20, 2021**, are not guaranteed to use the key.
- Enterprise tier.

## 5. Encrypting S3 Data with KMS (`s3a://` paths)

This page is about **your data buckets**, not the workspace storage bucket (that's the CMK workspace storage feature).

### 5.1 Unity Catalog approach (recommended)

1. **Update the KMS key policy** so S3 can use the key: allow `kms:Encrypt`, `Decrypt`, `ReEncrypt*`, `GenerateDataKey*`, `DescribeKey` for principals in your account, with conditions `kms:CallerAccount = <account ID>` and `kms:ViaService = s3.<region>.amazonaws.com`. (Skip if using SSE-S3 managed keys.)
2. **Create a storage credential** (IAM role in the **same account as the S3 bucket**). For SSE-KMS the IAM policy must include `kms:Decrypt`, `kms:Encrypt`, `kms:GenerateDataKey*` on the key.
3. **Create an external location**.
4. **Configure an encryption algorithm on the external location** (S3 only). This makes Databricks send the right encryption headers, which is needed if the bucket policy **requires encryption headers** on requests.

**Limitation:** SSE encryption is not supported with external tables shared using OpenSharing (the page's wording).

### 5.2 Instance profile approach (legacy)

- Databricks recommends Unity Catalog instead.
- Create an **instance profile**; add its IAM role as a **Key User** on the KMS key.
- Set Spark config (cluster config or init script):
  - `spark.hadoop.fs.s3a.server-side-encryption.key` = key ARN
  - `spark.hadoop.fs.s3a.server-side-encryption-algorithm` = `SSE-KMS`
- **Per-bucket** override: `spark.hadoop.fs.s3a.bucket.<bucket>.server-side-encryption.key`.
- Test on one cluster, then roll out via a **cluster-scoped init script in cluster policies**.

## 6. Encrypting Traffic Between Worker Nodes (In Transit)

**Status:** Public Preview; Enterprise plan.

**Default behavior:** user queries and transformations are typically sent to clusters over an encrypted channel, but **data exchanged between worker nodes (shuffles) is NOT encrypted by default**.

**How to enable:** a **cluster-scoped init script** (single cluster, or attach through cluster policy for all clusters) that configures Spark with:

- `spark.authenticate` = true plus a shared `spark.authenticate.secret`
- `spark.network.crypto.enabled` = true, `spark.network.crypto.saslFallback` = false (AES encryption, no fallback)
- `spark.ssl.*` with `spark.ssl.protocol` = TLSv1.3, keystore and passwords

The script has three jobs: (1) get the JKS keystore file and password, (2) set Spark **executor/worker** config (`spark-defaults.conf`), (3) set Spark **driver** config (`spark-branch.conf`, driver only). The keystore is copied once into DBFS.

**Trade-offs and warnings:**

- **Performance penalty** vs unencrypted traffic, especially with heavy shuffles (hardware acceleration helps but doesn't eliminate it).
- The shared secret is **derived from the hash of the keystore in DBFS**. Rotating it (updating the keystore) requires **restarting all running clusters**, or workers may fail to authenticate to the driver and jobs slow down.
- Because the secret derives from a file in DBFS, **any user with DBFS access can retrieve it from a notebook**.
- The JKS keystore is generated per workspace; its password is hard-coded and **not meant to protect confidentiality**.

**Alternative (no config needed):** certain AWS instance types **automatically encrypt traffic between worker nodes** (families listed on the page include M/C/R/D/I/G/P variants such as M5dn, M6i, C6i, R6i, I4i, G5, P4d).

**Disable:** remove the init script from the cluster config and restart.

**Doc inconsistency to know:** the parent overview page says **AES 128-bit over TLS 1.3**, while the dedicated page says **AES 256-bit over TLS 1.3**. If asked, cite the dedicated page (256-bit) but mention TLS 1.3 is the constant.

## 7. CMK for Unity Catalog (Per-Catalog Keys)

**Idea:** encrypt data in **individual catalogs** with **different keys** by sensitivity or compliance need (e.g., `finance-catalog-cmk`, `pii-data-cmk`).

**Scope and limitations:**

- Applies only to catalogs backed by **default storage**, not catalogs on external storage locations.
- Configurable via **account console or REST API only** (no Terraform support).
- Enterprise tier.

**Components:** AWS KMS keys → account-level **CMK configurations** (register the key with Databricks) → **catalog-level encryption settings**. Databricks root ARN must be granted access in the key policy.

**How it behaves:**

- Writes: Databricks encrypts with your key. Reads: Databricks **requests decryption from KMS**.
- **Revocation:** disable the key in AWS KMS and reads fail with a decryption error (there may be a delay). Re-enable the key to restore access.

**Setup flow:**

1. **Account admin** creates a CMK configuration (Security → Encryption keys; use case Managed services or Both; key ARN). Copy the config ID.
2. Grant the Databricks root ARN `kms:Encrypt`, `Decrypt`, `GenerateDataKey*`, `DescribeKey` in the key policy.
3. **New catalog:** `POST /api/2.1/unity-catalog/catalogs` with `"storage_mode": "DEFAULT_STORAGE"` and `"encryption_settings": {"customer_managed_key_id": "<cmk-id>"}`. Needs `CREATE CATALOG` + `USE CATALOG`.
4. **Existing catalog:** Catalog Explorer → catalog → Details → Advanced → Encryption settings → pick key. Needs `MANAGE` on the catalog or ownership.
5. **Verify:** `GET` the catalog and check `encryption_settings`.

**Critical gotchas:**

- Adding CMK to an existing catalog encrypts **only new data**. Existing data stays on Databricks-managed keys until you **rewrite** it.
- You can **change** the catalog's key at any time but **cannot disable CMK** once enabled.
- KMS key must be in an **active** state.

## 8. Credential Redaction

Databricks automatically redacts credentials in **audit logs** and **log4j Spark logs**. Redaction is based on the **value retrieved from the secret**, regardless of the variable or context it's used in.

| Credential | Replacement | Hash? |
| --- | --- | --- |
| AWS access key (starts with `AKIA`) | `REDACTED_AWS_ACCESS_KEY(hash_prefix)` | Yes |
| AWS secret access key | `REDACTED_POSSIBLE_AWS_SECRET_ACCESS_KEY` | **No** |
| Credentials in URI (`//user:pass@host`) | `REDACTED_CREDENTIALS(hash_prefix)` | Yes (hash of `user:pass`, **including the colon**) |
| Other cloud credentials | `[REDACTED]` or specific tokens like `REDACTED_POSSIBLE_CLOUD_SECRET_ACCESS_KEY` | varies |

- `hash_prefix` is a short **MD5**-based code; it lets you check the credential is valid/unaltered without exposing it.
- AWS has no explicit identifier for secret keys, so Databricks may **over-redact** random-looking 40-character strings (false positives). More broadly, long random-looking strings may be redacted.

## 9. Quick-Reference Cheat Sheet

| Question | Answer |
| --- | --- |
| Who creates key configs? | Account admin (account console or Account Key Configurations API) |
| Key region requirement? | Same region as the workspace; symmetric key |
| Databricks principal in key policy | AWS account `414351767826` (GovCloud variants differ) |
| Which condition scopes key use to your account? | `aws:PrincipalTag/DatabricksAccountId` |
| EBS statement principal | Your cross-account IAM role |
| Add storage key to existing workspace | CLI/API only; terminate compute first; up to 24h |
| Rotate managed services key | Yes, keep old key available 24h |
| Rotate storage key | No; use AWS auto-rotation |
| Serverless workspace keys needed | Managed services only |
| EBS CMK on serverless | N/A |
| Inter-node encryption default | Off |
| UC CMK scope | Default-storage catalogs only; no Terraform |
| UC CMK on existing catalog | New writes only; can't disable once on |
| Redaction hash algorithm | MD5 prefix |

## 10. Likely Interview Questions (with Short Answers)

1. **What's the difference between the managed services key and the workspace storage key?** Managed services protects control plane data (notebooks, secrets, SQL queries/history, Git creds, dashboards). Workspace storage protects the root S3 bucket (DBFS root, job/SQL results, MLflow models) and optionally cluster EBS volumes in your account.
2. **A customer needs everything encrypted with their key on a serverless workspace. What do you configure?** Only the managed services key; it covers default/root storage. EBS keys don't apply to serverless.
3. **How do you add a workspace storage CMK to an existing workspace?** Not via the console. Use CLI/API (`storage_customer_managed_key_id`), after terminating compute; expect up to 24h to take effect.
4. **Can you rotate the keys?** Managed services: yes, swap the config and keep the old key available for 24h. Storage: no swap; rely on AWS automatic rotation (same ARN).
5. **How do you guarantee a vendor can't read data after a breach/offboarding?** Revoke or disable the key in KMS. For UC CMK catalogs, reads then fail with decryption errors (with some delay).
6. **How do you encrypt shuffle traffic in a cluster?** Cluster-scoped init script setting Spark auth, `spark.network.crypto`, and TLS 1.3 SSL configs; or use instance types that encrypt inter-node traffic automatically. Expect a performance cost.
7. **What's the security weakness of the example init script?** Shared secret derives from a keystore hash in DBFS, so anyone with DBFS access can read it; rotation requires cluster restarts.
8. **How do you enforce SSE-KMS for external S3 data under Unity Catalog?** Update KMS key policy, storage credential IAM role with KMS permissions, external location, and set the encryption algorithm on the external location.
9. **You enabled CMK on an existing UC catalog. Is all data now encrypted with your key?** No. Only new writes; rewrite existing data to re-encrypt.
10. **Why does `statement_text` look empty in the query history system table after enabling CMK?** The managed services CMK encrypts it; configure a CMK on the `system` catalog to read it.
11. **How are credentials protected in logs?** Automatic redaction in audit and log4j logs, with hash prefixes for access keys and URI credentials, none for AWS secret keys.
