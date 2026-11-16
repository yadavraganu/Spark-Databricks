## 1. What Unity Catalog Is

Unity Catalog (UC) is the unified governance layer for data **and AI** in Databricks. When enabled for a workspace it operates beneath every data and AI interaction: enforcing access control when you query a table or call a model, tracking lineage, logging activity for auditing, and more. You work with its objects through Catalog Explorer, SQL, the CLI and REST APIs.

- **Automatically enabled** for all workspaces created after **November 8, 2023**; older workspaces must be upgraded.
- A unified model: ANSI-SQL-style `GRANT`/`REVOKE`, a three-level namespace (`catalog.schema.object`), automatic lineage down to the column level, audit logging, discovery, classification, quality monitoring, and open sharing.
- Governs **non-tabular and AI assets too**: volumes (files), functions, registered models, features, secrets, and AI **services** (model, model-provider, MCP and agent services).
- Also available as an **open-source implementation** (see §17).

**Governance capability map**

| Capability | Section |
|---|---|
| Access control (privileges, ABAC, row filters/column masks, workspace bindings) | §6–§8 |
| Governed tags, discovery | §9–§10 |
| Lineage | §11 |
| Data classification | §12 |
| Data quality monitoring | §13 |
| Auditing and system tables | §14 |
| Data sharing (OpenSharing, clean rooms) | §15 |
| Federation | §16 |
| AI governance (Unity Gateway) | §18 |

Account and metastore admins also get a consolidated view of the data estate (asset usage, classification coverage, data quality) on the **Data page in Governance Hub**.

## 2. The Object Model (Securable Objects)

Every governed asset is a **securable object**: something on which you can grant privileges to users, groups or service principals.

```
Metastore (one per cloud region)
├── Catalog                     (container)
│   └── Schema                  (container; a.k.a. database)
│       ├── Table               (managed | external | foreign)
│       ├── View                (standard | materialized view | metric view)
│       ├── Volume              (managed | external)
│       ├── Function            (UDFs, stored procedures, registered models)
│       ├── Model               (versioned/unversioned AI models)
│       ├── Service             (model, model provider, MCP, agent — Beta)
│       ├── Secret
│       └── Feature
├── Storage credential          ├── Share      ├── Provider
├── External location           ├── Recipient  └── Clean room
├── External metadata           ├── Connection
└── Service credential
```

- **Container objects** are catalogs and schemas: privileges granted on them are inherited by all current and future children, and they carry creation privileges (`CREATE SCHEMA`, `CREATE TABLE`, …).
- Objects directly under the metastore fall into two groups: those that **manage access to cloud storage and external systems** (storage credentials, external locations, external metadata, service credentials, connections) and those that **manage sharing across boundaries** (shares, providers, recipients, clean rooms).
- Each `information_schema` is auto-created in every catalog (read-only metadata views).

### Per-object details worth knowing

| Object | Key facts |
|---|---|
| **Table** | **Managed** (UC picks the storage path), **external** (you pick it; UC governs only), **foreign** (from a federated catalog, read-only). Access needs `USE CATALOG` + `USE SCHEMA` + `SELECT`/`MODIFY` |
| **View** | Read-only stored query, recomputed each time. The **view owner's** privileges resolve the underlying tables, so readers need no privileges on the base tables. Only the owner can edit it by default (transfer ownership to a group for collaborative editing) |
| **Materialized view** | Pre-computed; same model as views plus a `REFRESH` privilege to trigger refreshes |
| **Metric view** | Reusable metric definitions; query with `SELECT`; owner's privileges resolve sources |
| **Volume** | Unstructured files. `READ VOLUME` / `WRITE VOLUME`. Managed volumes recommended; external volumes can be bypassed by external systems |
| **Function** | UDFs, stored procedures, and registered models (models are a type of function). `EXECUTE` privilege |
| **Model** | `CREATE MODEL`, `CREATE MODEL VERSION`, `APPLY TAG`, `EXECUTE` |
| **Service (Beta)** | Governed invocable AI assets: model services, model provider services, MCP services, agent services. `CREATE SERVICE`, `EXECUTE` |
| **Secret** | `CREATE SECRET`, `READ SECRET`, `WRITE SECRET`, `REFERENCE SECRET` (lets a UC object use a secret without exposing its value), `APPLY TAG` |
| **Feature** | ML feature definition (metadata-only until materialized). `CREATE FEATURE`, `READ FEATURE` (metadata only — data still needs `SELECT` on the tables) |
| **Storage credential** | Auth info for a cloud path (an **IAM role** on AWS). `CREATE STORAGE CREDENTIAL` |
| **External location** | Storage credential + path. `CREATE EXTERNAL LOCATION`; `READ FILES` / `WRITE FILES` for direct file access |
| **External metadata** | Custom lineage for systems outside UC. `CREATE EXTERNAL METADATA` |
| **Service credential** | Auth for external cloud *services*. `CREATE SERVICE CREDENTIAL`, `ACCESS` |
| **Connection** | Endpoint + credentials for an external system: query/catalog federation, managed ingestion, JDBC, HTTP services. `CREATE CONNECTION`, `USE CONNECTION` |
| **Share / Provider / Recipient** | OpenSharing objects (`CREATE SHARE`, `CREATE PROVIDER`, `USE PROVIDER`, `CREATE RECIPIENT`). No privileges are granted on a recipient itself; `SELECT` on a **share** is granted *to* a recipient |
| **Clean room** | `CREATE CLEAN ROOM`, `EXECUTE CLEAN ROOM TASK`, `MODIFY CLEAN ROOM` |

## 3. Metastores

- A metastore contains **all securable objects registered in a single cloud region**. You need **one metastore per region** you operate in; many workspaces in that region attach to it.
- A metastore is a **logical boundary, not a service boundary** — it runs in a multi-tenant environment.
- **Grants apply to every attached workspace**: a privilege granted in one workspace is effective in all others sharing the metastore.
- **Metastore-level privileges do not inherit to children.** `CREATE CATALOG` lets you create catalogs; it grants no data access. (The one exception is `READ METADATA`, which inherits down when granted on a metastore.)
- The **metastore admin role is optional**. If an account admin creates the metastore manually (all metastores created before November 8, 2023), that account admin is the initial metastore owner and admin; if the metastore came from automatic enablement it has **no metastore admin**. Account admins assign the role — **to a group**, ideally — and changes can take ~30 seconds (longer in some workspaces) because of caching. See §6.4 for what the role can do and when you actually need one.
- When UC is enabled automatically (all workspaces created after **November 8, 2023**), **workspace admins** receive default metastore-level privileges through a system group; see §6.4. These **don't carry over** to other workspaces on the same metastore.
- **Isolation:** metastores give regional isolation but aren't the default data-isolation unit — **catalogs are**. Use one metastore for dev, test, prod and all business units in a region.
- **Never register a frequently accessed table as an external table in more than one metastore.** Schema/property/comment changes from writes in metastore A won't appear in B, and you can cause consistency issues with the Databricks commit service. Use OpenSharing between metastores.

### Managed storage (where managed tables and volumes live)
Managed storage can be set at the **metastore, catalog or schema** level, and data is stored at the **lowest available level** (schema → catalog → metastore). Databricks recommends **catalog-level storage as the primary unit of isolation**; metastore-level storage was required in early UC but **no longer is**.
- If you do use a metastore-level location, use a **dedicated bucket**.
- Don't use a bucket that's reachable from outside UC (it would let others bypass access control and auditing), and **never reuse a bucket that is or was your DBFS root**.

```sql
CREATE CATALOG production MANAGED LOCATION 's3://my-bucket/production/';
CREATE SCHEMA production.secure_data MANAGED LOCATION 's3://my-bucket/secure/';  -- physically isolates this schema
```

## 4. Catalogs and Schemas

- **Catalogs** are the primary unit of data isolation: typically one per environment, team or business unit. Because grants inherit, owning a catalog or holding broad privileges on it is very powerful.
- **Schemas** are the second level — usually a use case, project or team sandbox (also a common place for bronze/silver/gold layers). `CREATE DATABASE` is an alias for `CREATE SCHEMA`.
- Special catalog types: **foreign catalog** (Lakehouse Federation only) and **`hive_metastore`** (surfaces legacy Hive metastore data; deprecated — migrate to UC).
- `USE CATALOG` / `USE SCHEMA` are prerequisites for anything inside them.
- **`BROWSE`** (catalog-level only) lets users see metadata for every object in the catalog without data access. Databricks recommends granting it to **`All account users`** so data is discoverable and users can request access.
- **Workspace-catalog binding** restricts which workspaces can reach a catalog (optionally **read-only** per workspace). It **supersedes privilege grants**: an explicit `SELECT` doesn't help if the catalog isn't bound to your workspace. Unbound catalogs appear grayed out (visible to metastore admins and the catalog's owners) with no queryable child objects. Information schema, lineage and Catalog Explorer show only catalogs bound to the current workspace. Bindings also exist for **external locations, storage credentials and service credentials**.
  - Needed to define or edit bindings: metastore admin, catalog owner, or `MANAGE` on the catalog; **`READ METADATA`** is enough to *view* them. CLI: `databricks catalogs update <catalog> --isolation-mode ISOLATED`, then `databricks workspace-bindings update-bindings catalog <catalog> --json '{...}'` with `BINDING_TYPE_READ_WRITE` (default) or `BINDING_TYPE_READ_ONLY`.
  - The auto-created **workspace catalog** is bound only to its own workspace. If you unbind it or share it, grant permissions explicitly, because the workspace-admins group is **workspace-local**.
- A schema or catalog per team: grant `USE SCHEMA` + `CREATE TABLE` only to that team (plus `USE CATALOG` on the parent).

## 5. Tables, Volumes and Storage Objects

### Managed vs. external

| | Managed | External |
|---|---|---|
| Storage location | Chosen by UC (in **your** cloud account) | Chosen by you |
| File lifecycle (optimization, layout, deletion) | UC | You |
| Format | **Delta or Apache Iceberg** | Delta, CSV, JSON, Avro, Parquet, ORC, … |
| `DROP` behavior | Data files **permanently deleted after an 8-day retention period** | Files stay in place |
| Ownership of data | Yours (never held by Databricks) | Yours |

- Databricks recommends **managed tables for all new tables**: auto compaction, auto optimize, metadata caching, intelligent file sizing — new functionality gives precedence to managed tables.
- Both managed and external tables support read/write/create from **external engines through open APIs** (the Unity REST API and Iceberg REST Catalog), so managed tables don't mean lock-in.
- Use **external tables** when upgrading from Hive metastore (one-click, no data movement), when disaster-recovery needs can't be met by managed tables (managed tables can't be registered across metastores), when external readers/writers must touch the data, or for non-Delta/Iceberg formats. Create them with **one external location per schema**.
- **Foreign tables** come from federated catalogs and are read-only.

### Volumes
- **Managed volumes**: UC governs access to the storage. **External volumes**: register existing files (location must sit under an external location) — good for landing zones, staging areas for Auto Loader/`COPY INTO`, and files produced by other systems. External systems can bypass UC governance on external volumes.
- Volumes expose a FUSE path: `/Volumes/<catalog>/<schema>/<volume>/`.

### External locations — use sparingly
- An external location is broad (a bucket or prefix). Give `CREATE EXTERNAL LOCATION` only to trusted admins/data engineers.
- **Don't grant `READ FILES`/`WRITE FILES` to end users.** Use external locations only to register external tables/volumes, register managed storage (`CREATE MANAGED STORAGE`), or explore files before registering. For path-based access to files, use **volumes** — access via the cloud URI is governed by privileges on the *volume*, not the location.
- Never create external tables/volumes at the **root** of an external location (use a subdirectory), don't mount the same storage to DBFS, limit direct bucket access, and **enable file events** for faster Auto Loader and file-arrival triggers.

### Constraints and query optimization
Primary and foreign key constraints are **informational (not enforced)**. `RELY` lets the optimizer trust a primary key (e.g. to remove redundant `DISTINCT`/joins), and Catalog Explorer can draw an ER diagram from them. **Predictive optimization** runs `OPTIMIZE`/`VACUUM` on managed tables automatically (settable at metastore, catalog, schema or table level).

```sql
ALTER TABLE production.gold.customers ADD CONSTRAINT pk_customer PRIMARY KEY (customer_id);
ALTER TABLE production.gold.daily_revenue SET PREDICTIVE OPTIMIZATION ENABLED;
```

### Storage credentials and external data access
```sql
CREATE STORAGE CREDENTIAL my_cred WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789012:role/unity-catalog-role');
CREATE EXTERNAL LOCATION raw_data_loc URL 's3://my-bucket/raw/' WITH (STORAGE CREDENTIAL my_cred);
```
To prevent accidental exfiltration, **schema owners don't hold `EXTERNAL USE SCHEMA`** and **external location owners don't hold `EXTERNAL USE LOCATION`** by default — these must be granted explicitly before external engines can access data through UC.

## 6. The Privilege Model

### 6.1 Core rules
- Access = **`USE CATALOG`** on the catalog **+ `USE SCHEMA`** on the schema **+** the object-level privilege. All three are required — `SELECT` alone isn't enough.
- Usage privileges are a control for administrators: even if a table owner wants to share a table, users can't reach it without usage privileges on the parents (which only catalog/schema owners or `MANAGE` holders can grant).
- Privileges **inherit downward**, including **denials**; ownership does **not** inherit.
- A user or group must be explicitly granted a privilege to perform an action.

| Operation | Required |
|---|---|
| Read a table/view | `USE CATALOG`, `USE SCHEMA`, `SELECT` |
| Write a table | `USE CATALOG`, `USE SCHEMA`, `MODIFY` |
| Create a schema | `USE CATALOG`, `CREATE SCHEMA` on the catalog |
| Create a table | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` |
| Execute a function | + `EXECUTE` |
| Read volume files | + `READ VOLUME` |

```sql
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG sales TO `finance_team`;           -- all current and future tables/views
GRANT USE CATALOG, USE SCHEMA, CREATE TABLE ON CATALOG analytics TO `data_engineers`;
```

### 6.2 Special privileges

| Privilege | Meaning |
|---|---|
| `MANAGE` | Grant/revoke privileges, transfer ownership and drop an object **without being the owner**. Not data access (holders can grant it to themselves). **On a container it applies to all children.** Needs usage privileges only on parents *above* the object (`MANAGE` on a catalog needs none) |
| `BROWSE` | See that an object exists, its name, description and tags, and **request access** — no `USE CATALOG`/`USE SCHEMA` needed. Data objects: **catalog level only**. External locations, external metadata and clean rooms: grantable on the object itself |
| `READ METADATA` | Child of `MANAGE`: read-only view of **owner-visible metadata, including sensitive items** (grants, row filters/column masks, ABAC policies, view/function definitions, credential details). Meant for auditors, governance teams and SREs — grant sparingly; use `BROWSE` for ordinary discovery. Inherits down when granted on a metastore, catalog or schema |
| `MANAGE ACCESS CONTROL` | Child of `MANAGE`: grant/revoke, transfer ownership, manage ABAC policies, access-request destinations, and (on a catalog) workspace bindings. Can be **denied** with an ABAC DENY policy, never granted |
| `ALL PRIVILEGES` | Implies all applicable privileges for the object type but **excludes** `MANAGE`, `READ METADATA`, `EXTERNAL USE SCHEMA` and `EXTERNAL USE LOCATION`. It is evaluated at check time, so it automatically covers new privileges and new child objects. `SHOW GRANTS` returns only `ALL PRIVILEGES`; revoking it removes the implied privileges but not those four |
| `APPLY TAG` | Add/edit tags (also column-level tags on tables/views, version-level on models). Governed tags additionally require `ASSIGN` on the tag |
| `INSERT` / `UPDATE` / `DELETE` **(Beta)** | Fine-grained DML children of `MODIFY` |
| `MODIFY` | Insert, update and delete table data (needs `SELECT` too). Can't be granted on foreign tables (read-only); also lets users add lineage to external metadata |
| `EXECUTE` | Invoke a function or load a registered model for inference; also lets you see function definitions or model-version metadata. Databricks recommends granting it on individual functions |
| `REFRESH` | Refresh a materialized view |
| `READ FILES` / `WRITE FILES` | Direct cloud-storage access via an external location. **`WRITE FILES` also requires `READ FILES`** (otherwise `PERMISSION_DENIED`). Prefer volumes |
| `EXTERNAL USE SCHEMA` / `EXTERNAL USE LOCATION` | Allow credential vending to external engines (Unity open APIs / Iceberg REST). Never implied by ownership or `ALL PRIVILEGES`; **only the catalog owner can grant `EXTERNAL USE SCHEMA`** (Public Preview) and only `MANAGE` holders on the location can grant `EXTERNAL USE LOCATION` |
| `MANAGE ALLOWLIST` | Metastore-level: edit the allowlist of init scripts, JARs and Maven coordinates for **standard access mode** compute (empty by default). Grant only to metastore admins/trusted platform admins |

**Creation privileges** (all need usage privileges on the parents): `CREATE CATALOG` (metastore), `CREATE SCHEMA` (catalog), `CREATE TABLE` (tables *and* views — and it authorizes view creation), `CREATE VOLUME`, `CREATE FUNCTION`, `CREATE MODEL`, `CREATE MODEL VERSION`, `CREATE MATERIALIZED VIEW`, `CREATE FEATURE`, `CREATE SECRET`, `CREATE SERVICE` (schema/catalog); `CREATE EXTERNAL TABLE` and `CREATE EXTERNAL VOLUME` (on an **external location** — preferred over granting on a storage credential because it's path-scoped), `CREATE MANAGED STORAGE` (location → custom managed storage for a catalog/schema), `CREATE FOREIGN CATALOG` (connection), `CREATE FOREIGN SECURABLE` (location → authorized paths), `CREATE EXTERNAL LOCATION` (needed on both the metastore **and** the storage credential), `CREATE CONNECTION`, `CREATE STORAGE CREDENTIAL`, `CREATE SERVICE CREDENTIAL`, `CREATE EXTERNAL METADATA`, `CREATE CLEAN ROOM` (metastore).

**OpenSharing/Marketplace privileges:** `CREATE SHARE`, `CREATE RECIPIENT`, `CREATE PROVIDER`; `USE SHARE`, `USE RECIPIENT`, `USE PROVIDER` (read-only visibility; `USE PROVIDER` + `CREATE CATALOG` lets a non-admin mount a share as a catalog); `SET SHARE PERMISSION` (grant recipients access to a share, with `USE SHARE` and `USE RECIPIENT`/ownership); `USE MARKETPLACE ASSETS` (on by default); clean rooms: `EXECUTE CLEAN ROOM TASK`, `MODIFY CLEAN ROOM`.

Composite and child privileges are granted and revoked independently: granting a composite doesn't grant its children, and revoking it doesn't revoke explicitly granted children. A `REVOKE` succeeds even if the privilege was never granted.

### 6.3 Ownership vs. `MANAGE`

| | Owner | `MANAGE` |
|---|---|---|
| All object capabilities | Implied (not shown by `SHOW GRANTS` as `ALL PRIVILEGES`) | No — must be granted separately |
| Grant/revoke, transfer ownership, drop | Yes | Yes |
| Manage child objects | Implied (but not ownership of them) | Explicitly granted on children |
| Number of principals | One | Many |

The creator of an object is its first owner and should reassign it to a group. Privileges can be granted by: the object's owner, the owner of its catalog or schema, a `MANAGE` holder, or a metastore admin; only a metastore admin or account admin can grant on the **metastore** itself.

**Transferring ownership**
- Allowed for the current owner, a metastore admin, the owner of the container, or a `MANAGE` holder.
- To prevent privilege escalation, only a **metastore admin** can transfer a **view, function or model** to any principal; owners and `MANAGE` holders can only transfer to themselves or a group they belong to. Share ownership transfers are metastore-admin-only.
- Transferring a view/metric view to a **group** enables collaborative editing while data access stays limited to what the group can see.
- Materialized views and streaming tables from **Lakeflow pipelines** can't be transferred directly — change the pipeline's **run-as** user and the owner updates on the next refresh.

```sql
ALTER TABLE production.sales.orders OWNER TO `data-engineering-leads`;
SHOW GRANTS ON SCHEMA main.default;
GRANT EXECUTE ON FUNCTION prod.ml_team.iris_model TO `ml-team-acme`;   -- models are functions
```
Note: `MANAGE` holders currently see only their *own* grants in `INFORMATION_SCHEMA` (not all grants); use `SHOW GRANTS`/Catalog Explorer.

### 6.4 Admin roles

| Role | Scope | Required? | Purpose |
|---|---|---|---|
| **Account admin** | Whole account | Yes | Create metastores and workspaces, link them, assign admin roles, grant on metastores, enable OpenSharing for a metastore, configure storage credentials, enable system tables |
| **Workspace admin** | One workspace | Yes | Workspace membership, job ownership and Run-as, workspace objects; create catalogs and other top-level securables (workspaces created after Nov 8, 2023) |
| **Metastore admin** | One metastore | **No** | Govern top-level UC securables; owns the metastore |

- Account admins can **restrict workspace admin privileges** with the `RestrictWorkspaceAdmins` setting.
- **Default workspace-admin privileges** (auto-enabled workspaces): `CREATE CATALOG`, `CREATE CLEAN ROOM`, `CREATE EXTERNAL LOCATION`, `CREATE SERVICE CREDENTIAL`, `CREATE STORAGE CREDENTIAL`, `CREATE CONNECTION`, `CREATE SHARE`, `CREATE RECIPIENT`, `CREATE PROVIDER`, granted to a system group named `_workspace_admins_<workspace_name>_<workspace_id>`. **If you revoke one from that group you can't grant it back to it** — grant it to a different principal. Workspace admins also own the workspace catalog (they can manage its privileges and grant themselves access; grants are audit-logged), and all workspace users get `USE CATALOG` on it plus `USE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`, `CREATE MODEL`, `CREATE FUNCTION`, `CREATE MATERIALIZED VIEW` on its `default` schema.
- **Metastore admin defaults:** `CREATE CATALOG`, `CREATE CLEAN ROOM`, `CREATE CONNECTION`, `CREATE EXTERNAL LOCATION`, `CREATE SERVICE CREDENTIAL`, `CREATE STORAGE CREDENTIAL`, `CREATE FOREIGN CATALOG`, `CREATE SHARE`, `CREATE RECIPIENT`, `CREATE PROVIDER`, `CREATE MATERIALIZED VIEW`, `MANAGE ALLOWLIST`; plus, as **owner of the metastore**: manage privileges and transfer ownership of anything in it, indirectly grant data access (via taking ownership — no direct access by default, and grants are audit-logged), read/update all object metadata, set tags, configure default access-request destinations, and delete the metastore. Metastore admins are **always exempt** from ABAC DENY policies.
- **You need a metastore admin only to:** change ownership of or grant on objects you don't own (e.g. taking over a catalog after its owner leaves), remove the default workspace-admin permissions, add managed storage to a metastore that has none, or enable default access-request destinations.
- Keep these groups small; push day-to-day governance to catalog and schema owners.

### 6.5 Access requests (Public Preview)

The request-for-access (RFA) feature lets users request privileges on objects they can discover with `BROWSE` (or reach through a direct URL).

- **Destinations:** email, **Slack**, **Microsoft Teams**, **webhook** (JSON payload with requester, object, privileges, principal and a deep link) or a **redirect URL** to your own system (a URL excludes all other destinations). Configure on metastores, catalogs, schemas, tables, views, volumes, functions, models, storage/service credentials, external locations and connections — via Catalog Explorer (*Manage access request destinations*), REST (`PATCH /api/3.0/rfa/destinations`) or Terraform (`databricks_rfa_access_request_destinations`). Needs ownership, `MANAGE`, or metastore admin.
- **Inheritance and resolution:** a destination set high in the hierarchy applies to children without their own. With **metastore-level** requests enabled (recommended; a metastore admin enables it), the order is: object's explicit destination → object owner's email (if an individual) → nearest parent destination → metastore-level destination. With none, users can't request access.
- **Where users request:** Catalog Explorer, `INSUFFICIENT_PERMISSIONS` errors in the SQL editor/notebooks, AI/BI dashboards, and Genie agents. Missing `USE CATALOG`/`USE SCHEMA` automatically generates extra requests to the parent approvers.
- **Approving:** follow the notification link, then either **add the principal to a group** that holds the privilege or **grant privileges directly** (presets such as *Data Reader*). Events are in the audit log (request-for-access events).

## 7. Choosing an Access-Control Mechanism

| Mechanism | Applies to | Defined with | Use for |
|---|---|---|---|
| **Privileges** | Catalogs, schemas, tables… | `GRANT`/`REVOKE`, ownership | Baseline access and delegation |
| **ABAC policies** | Tagged objects | Governed tags + policies (UDFs for filter/mask; none for GRANT/DENY) | Centralized, tag-driven, dynamic enforcement |
| **Table-level row filters / column masks** | One table/column | UDF via `ALTER TABLE` | Table-specific logic |
| **Dynamic views** | A view over base tables | SQL (`is_account_group_member()` etc.) | Curated/transformed slices of data |
| **Workspace bindings** | Catalogs, external locations, storage credentials | Workspace assignment | *Where* data can be reached from |

Databricks **recommends ABAC** to centralize and scale access control; use table-level filters and masks only for per-table logic or if you haven't adopted ABAC.

## 8. Fine-Grained Access Control

### 8.1 Attribute-based access control (ABAC)
Access is decided by evaluating **governed tags** (attributes) in **policy** conditions. A policy is attached to a securable and applies to every matching object beneath it, evaluated dynamically at access time — a newly created and tagged object is covered automatically, much like a "future grant".

**Four policy types**

| Type | What it does | UDF? | Attach at |
|---|---|---|---|
| **Row filter** | UDF returns boolean per row; `FALSE` rows are excluded | Yes | Metastore (Beta), catalog, schema, table |
| **Column mask** | UDF takes the column value (bound from `ON COLUMN`) and returns the original or masked value; return type must be castable to the column type | Yes | Metastore (Beta), catalog, schema, table |
| **GRANT** | Dynamically grants privileges on **AI securables** (models, model/model-provider/MCP services; agent services and skills in Beta) whose tags match. Effective privileges = **union** of direct grants and GRANT policies; it can't revoke | No (inline condition) | Metastore (Beta), catalog, schema |
| **DENY (Beta)** | Denies **`MANAGE ACCESS CONTROL`** on a securable type; **always overrides any grant**, including via ownership | No | Metastore, catalog, schema |

Row filter and column mask policies apply to **tables** (including streaming tables, materialized views, and views in Beta) via `FOR TABLES`. Databricks recommends attaching policies at the **highest applicable level** (usually the catalog).

**Syntax (correct form — `TO` and `FOR TABLES` are required)**
```sql
CREATE FUNCTION ssn_to_last_nr (ssn STRING, nr INT) RETURNS STRING RETURN right(ssn, nr);

CREATE POLICY mask_ssn
ON SCHEMA prod.customers
COLUMN MASK ssn_to_last_nr
TO us_analysts EXCEPT admins
FOR TABLES
MATCH COLUMNS has_tag_value('pii', 'ssn') AS ssn
ON COLUMN ssn
USING COLUMNS (4);

-- Table scoped by tag, column matched by tag
CREATE POLICY mask_pii_for_hr
ON CATALOG catalog_a
COLUMN MASK mask_pii
TO `account users` EXCEPT `HR admins`
FOR TABLES
WHEN has_tag('HR')
MATCH COLUMNS has_tag('PII') AS pii_col
ON COLUMN pii_col;

-- Row filter
CREATE POLICY regional_access_emea
ON CATALOG sales
ROW FILTER filter_by_region
TO `emea team`
FOR TABLES
MATCH COLUMNS has_tag('region') AS rgn
USING COLUMNS (rgn, 'EMEA');

-- GRANT policy (AI assets)
CREATE POLICY grant_production_model_access
ON SCHEMA production.ml_models
TO `analysts`
GRANT EXECUTE FOR MODELS
WHEN has_tag_value('lifecycle', 'production');

-- DENY policy (Beta): non-admins can't manage access on sensitive tables
CREATE POLICY deny_manage_access_control_sensitive_tables
ON CATALOG catalog_a
COMMENT 'Prevent non-admins from managing permissions on sensitive tables'
TO `account users` EXCEPT `data_admins`
DENY MANAGE ACCESS CONTROL
FOR TABLES
WHEN has_tag('sensitive');
```
- `WHEN` filters **which tables** by tags (default `TRUE`). `MATCH COLUMNS` picks **which columns**: **up to 6 expressions for a row filter, up to 3 for a column mask**, all must match, each with an alias used in `ON COLUMN`/`USING COLUMNS`. If a table lacks a column matching every condition, the policy simply doesn't apply (data comes back unmasked).
- `USING COLUMNS` accepts column aliases, constants (strings, numbers, booleans, `NULL`) and **tag introspection functions**: `get_tag_value('key')` (table tag, including inherited) and `get_column_tag_value(alias, 'key')` (a matched column's own tag). One policy and one UDF can then handle many tag values (e.g. a single `mask_pii` UDF that branches on `email`/`ssn`/`phone`). Creating such a policy needs DBR 18 LTS+.
- Condition functions: `has_tag('k')`, `has_tag_value('k','v')` (snake_case; the camelCase forms still work). In `WHEN` they check tags set on the table **or inherited**; in `MATCH COLUMNS` they check **column-level tags only** — tags don't propagate from table to column.
- Policy updates use `CREATE OR REPLACE POLICY`; `DROP POLICY name ON SCHEMA x`. SQL needs `FOR` with the **plural** type (`FOR TABLES`, `FOR SCHEMAS`).
- Manage: `SHOW [EFFECTIVE] POLICIES ON { METASTORE | CATALOG | SCHEMA } name`, `DESCRIBE POLICY`, `INFORMATION_SCHEMA.ABAC_POLICY_DEFINITIONS`, REST (`/api/2.1/unity-catalog/policies`), Terraform, SDKs. `SHOW GRANTS` does **not** show GRANT/DENY policies.
- **Permissions:** create/edit/delete needs `MANAGE` on (or ownership of) the attach point, plus `EXECUTE` on the UDF; viewing needs `READ METADATA` or `MANAGE`. Metastore-level policies need metastore admin. Policy events are audit-logged (`createPolicy`, `deletePolicy`, `getPolicy`, `listPolicies`).

**Attribute sources beyond tags**
- **Identity attributes (Beta)** — `has_identity_attribute_value('k','v')`, `has_identity_attribute_tag_match('k','tagKey')`; work in `WHEN` of **column mask** policies; need an account-admin preview and identity-attribute provisioning from your IdP. A missing attribute resolves to **`false`**, so phrase conditions so `false` restricts. Keys/values case-sensitive; IdP changes aren't instant; never store sensitive data in attributes.
- **Context attributes (Beta)** — `has_context_attribute('k')`, `has_context_attribute_value('k','v')` with `request.is_on_behalf_of` and `request.client_id`; let you restrict **agents acting on behalf of a user through an OAuth app** while the user's direct queries stay unrestricted. Preview must be enabled; disabling it while policies use these functions makes queries fail (`UC_ABAC_CONTEXT_ATTRIBUTE_FUNCTIONS_DISABLED`).

**DENY policy rules and caveats (Beta)**
- Only `MANAGE_ACCESS_CONTROL` can be denied, on catalogs, schemas, tables, volumes, functions, models and secrets; one securable type per policy (use `FOR SCHEMAS` to cover everything within a schema). It inherits downward, overlapping DENYs **union** (most restrictive wins), and it's independent of grants (it can deny what was never granted; removing a grant doesn't remove it).
- **Metastore admins are always implicitly exempt**, guaranteeing a path to fix or remove any DENY. Always put a trusted group in `EXCEPT` when targeting `account users`, and attach at the smallest sufficient scope.
- A denied principal loses grant/revoke, ownership transfer, ABAC policy management, access-request destinations and (at catalog level) workspace-binding management — but still sees metadata.
- It does **not** stop: `CTAS` or creating views over data you can `SELECT` (don't grant `CREATE TABLE` unnecessarily), applying/removing governed tags (limit `ASSIGN`/`APPLY TAG`), OpenSharing (limit `CREATE SHARE`), credential vending (don't grant `EXTERNAL USE SCHEMA`), or group-membership changes (limit **Manage** on groups).
- SQL needs classic compute on **DBR 18 LTS+**. GRANT and DENY share one quota per object.

**Evaluation, enforcement and fail-closed behavior (row filters/masks)**
- Unity Catalog evaluates: policies whose scope covers the table → is the user in `TO` and not `EXCEPT` → do tags/attributes match → send the effective filter/mask to the runtime, which enforces it like a secure view. Results differ per user because evaluation depends on identity, group membership and tags.
- **Through a view:** access to base tables is checked as the **view owner**, but policies are evaluated for the **querying user**.
- **Fail closed:** unsupported runtimes, unsupported operations and **removed dependencies** deny access. Deleting a **governed tag** referenced by a policy makes every query in scope fail (`UC_ABAC_UNKNOWN_TAG_POLICY`) — even on untagged tables — and the UI/API won't stop you; the tag also becomes ungoverned. Dropping a policy's **UDF** fails queries with `UC_DEPENDENCY_DOES_NOT_EXIST`. A **tagged column can't be dropped** until the tag is removed (this blocks pipelines that drop columns; remove the tag, run, then re-tag).
- **Conflicts:** only one distinct row filter per user/table and one distinct mask per column. Conflicts (including a manual table-level filter/mask plus an ABAC one, a `USING COLUMNS` alias matching several columns, or a masked column used as another policy's input) **block access** with `UC_ABAC_MULTIPLE_ROW_FILTERS` / `MULTIPLE_MASKS`. Diagnose with `SHOW EFFECTIVE POLICIES`, `INFORMATION_SCHEMA.ROW_FILTERS`/`COLUMN_MASKS`, and refine conditions, tags or principals.
- **Type casting:** mask inputs/outputs are auto-cast (ANSI `CAST` rules); policies can also cast complex columns to `VARIANT` (`STRUCT` on DBR 18.1+, `ARRAY`/`MAP<STRING,…>` on DBR 19+). Time-travel queries on qualifying tables enforce policies.

**Requirements**
- **Serverless**, or **standard compute on DBR 16.4+**, or **dedicated compute on DBR 16.4+** (enforcement is delegated to serverless). Older runtimes **can't read ABAC-secured tables** — exempt them via `EXCEPT` during migration.
- **Governed tags only** (not ungoverned tags); tag changes take a few minutes.
- GRANT policies via SQL need classic compute on DBR 18 LTS+.

**Quotas:** 10,000 policies per metastore; 100 attached directly to the metastore (Beta); 100 per catalog; 100 per schema; **50 per table**; 20 principals per policy.

**Limitations and gotchas**
- **GRANT policies don't grant `USE CATALOG`/`USE SCHEMA`**, `MANAGE`, `MODIFY` or `ALL PRIVILEGES`, and union with direct grants (don't mix both for the same privilege).
- **Materialized views and streaming tables:** the refresh runs as the pipeline's owner/run-as identity; if it's subject to a mask or filter, the stored result is **permanently masked/filtered**. Add the pipeline identity to `EXCEPT`.
- **Clones** (deep/shallow), **AI Search index** creation/sync and **OpenSharing** need the acting principal (e.g. the share owner) in `EXCEPT` — and OpenSharing policies don't govern recipient access.
- **ABAC on views (Beta):** needs a workspace-admin preview and **DBR 19+**; max 20 stacked policies per query; `CREATE OR REPLACE VIEW` drops tags and policies (use `ALTER VIEW`); metric views unsupported.
- **AI Search indexes ignore ABAC** on the source table (exclude masked columns from sync).

**Separation of duties** — a typical split: (1) account admins or tag-`CREATE` holders define the **tag taxonomy**; (2) stewards/data creators/classification apply tags (needs `ASSIGN` on the tag **and** `APPLY TAG` on the object — *tagging is a security boundary*, so control and audit it); (3) governance admins write policies (`MANAGE` + `EXECUTE` on the UDF); (4) data creators create objects that inherit tags (they get `APPLY TAG` on what they create); (5) consumers query — row filter/mask policies need a direct `SELECT` grant first, because they don't grant access themselves.

### 8.2 Table-level row filters and column masks
```sql
CREATE FUNCTION mask_ssn(ssn STRING) RETURNS STRING
RETURN CASE WHEN is_account_group_member('hr-admins') THEN ssn ELSE '***-**-****' END;

ALTER TABLE production.hr.employees ALTER COLUMN ssn SET MASK mask_ssn;
ALTER TABLE employees SET ROW FILTER salary_filter ON (department);
```
- Managed by the table owner or a `MANAGE` holder; one mask per column; only one distinct row filter per user and table.
- **Parameter types must match the column type.** With ANSI mode off, a mismatch silently casts to `NULL` and can make a filter return **all rows**. Enable ANSI mode.
- **Limitations:** can't apply to views; no Iceberg REST / Unity REST access to filtered tables; no Delta Lake APIs; no path-based access; no time travel (ABAC time travel is Beta); no clones; OpenSharing providers can't share tables with *table-level* filters/masks; columns referenced by generated columns can't be masked; DBR < 12.2 fails securely.
- **Performance:** use simple SQL UDFs with few arguments, few distinct masks, `try_divide`-style deterministic error-safe expressions, and avoid long `AND` chains; Python UDFs are slower.

### 8.3 Dynamic views
Wrap base tables in a view that filters/masks using group-membership functions. Best for sharing a curated or joined slice to users with no access to the base tables.

## 9. Governed Tags

- **Account-level** tags with a built-in **tag policy**: you define the key, the **allowed values** (max 500) and **who can assign/manage** it. Max **1,000 governed tags per account**. Enforced across all workspaces.
- Apply to UC objects (catalogs, tables, columns…) **and** workspace objects such as apps, dashboards, Genie agents and notebooks — **not** compute (SQL warehouses) or jobs, which use a separate mechanism.
- **Inheritance:** objects inherit governed tags from their catalog/schema, **except individual table columns**.
- Creating a governed tag whose key matches existing tags **automatically governs those existing assignments**.
- Keys/values: ≤256 chars, UTF-8, **case-sensitive**, no `* . / < > % & ? \ =`, no leading/trailing whitespace.
- **Tag data is plain text and may be replicated globally** — never put sensitive information in tag names or values.
- **System governed tags** (can't be edited; wrench icon): `system.certification_status` (`certified`/`deprecated`), `class.*` (classification, e.g. `class.email_address`, `class.us_ssn`), `sap.PersonalData.*`, `ai.*` (e.g. `ai.model_creator`, `ai.model_family` on foundation models in `system.ai`).
- **Automated tagging:** Data Classification (columns), custom classifiers (Beta), **automate tag assignment** (Beta; certify/deprecate, flag missing tags, sensitivity tiers by deterministic rules), and **tag propagation** (Beta; copies a governed tag along lineage to derived tables/columns).

```sql
ALTER TABLE production.sales.orders SET TAGS ('domain' = 'sales');
ALTER TABLE production.hr.employees ALTER COLUMN ssn SET TAGS ('pii' = 'ssn');
```

## 10. Data Discovery

Catalog Explorer is the UI for discovering and managing UC assets (search, comments, AI-generated comments, tags, **certified/deprecated** badges, table **Insights** with usage and popularity). Make data discoverable with `BROWSE` for **All account users**, consistent governed tags, and configured access-request destinations (§6.5).

## 11. Lineage

UC captures lineage **automatically for queries run on Databricks, down to the column level**, aggregated across all workspaces on the metastore. Uses: impact analysis, root-cause investigation, sensitive-data flow tracking, cross-team dependencies.

- **Nodes:** tables, views, ML model versions, external assets, file paths, UDFs, jobs, notebooks, pipelines, dashboards, and **model services / model providers** (with their inference tables).
- **External lineage:** register upstream sources (Salesforce, MySQL) and downstream tools (Tableau, Power BI) as **external metadata** objects to see them in the same graph.
- **To view lineage** you need at least **`BROWSE`** on the parent catalog (and the catalog must be bound to your workspace); users without access see **masked nodes**.
- **Query it:** `system.access.table_lineage` and `system.access.column_lineage` (rolling **1-year** window). Catalog Explorer retains lineage indefinitely from Sept 1, 2024 (default view: 1 year).
- **Genie Code:** `/getTableLineages`, `/getTableInsights`.
- **Requirements:** tables registered in UC; Spark DataFrame or Databricks SQL interfaces; DBR 11.3 LTS+ for streaming lineage, 13.3 LTS+ for pipeline column lineage; allow the **Kinesis endpoint** in your firewall/VPC if you use a customer-managed VPC or PrivateLink.
- **Limitations:** no lineage for **RDDs**, global temp views, checkpointed datasets, `system.information_schema`, or **path-referenced** tables (column lineage needs table names); **not preserved on rename**; jobs via `runs submit`/`spark submit` lose the job link; UDF lineage is table-level only; rolled-back transactions still emit lineage; lineage doesn't cross metastores or Databricks-to-Databricks shares.

## 12. Data Classification

Uses an **agentic AI system (with an LLM)** to automatically detect sensitive data (PII etc.) in Unity Catalog tables and tag columns with `class.*` governed tags, so you can then govern them with ABAC.

- **Enable:** per catalog (owner or `MANAGE`), with optional schema scope and a usage policy. Initial scan is the costliest; later scans are **incremental**; new tables/columns are typically scanned within **24 hours**. Requires serverless compute.
- **Results:** a results page (detected columns, auto-tagging status, compliance framework, **unmasked user access in the last 7 days**) and the system table `system.data_classification.results` (account-admin only by default; it contains **sample values**).
- **Auto-tagging** can be set at **metastore** or **catalog** level (catalog overrides). Not backfilled immediately — takes effect on the next scan (within ~24 hours). Disabling stops new tags but doesn't remove existing ones.
- **Exclude detections (Beta)** to remove a wrong tag, prevent it returning and improve accuracy.
- **Views (Beta):** scanned with a preview, but `class.*` tags aren't applied to view columns unless ABAC on views is on; metric views and views with unsafe definitions aren't scanned.
- Supports frameworks such as PII, PCI DSS, GDPR, HIPAA, GLBA, DPDPA and PIPEDA.
- **Permissions:** by default only **account admins** hold `MANAGE`/`ASSIGN` on the classification system governed tags.
- **Cost:** query `system.billing.usage` with `billing_origin_product = 'DATA_CLASSIFICATION'`; results are stored in default storage at no charge.

## 13. Data Quality Monitoring

Formerly Lakehouse Monitoring (for the profiling part). Runs on **serverless**, billed under the **`DATA_QUALITY_MONITORING`** product. It doesn't modify monitored tables or add overhead to the jobs that write them.

- **Anomaly detection** — one-click, schema-level monitoring with **intelligent scanning** that prioritizes important tables. Checks **freshness** (a per-table model predicts the next commit; unusually late = stale) and **completeness** (expected row-count range for the last 24 hours; below the lower bound = incomplete).
- **Data profiling** — table-level summary statistics, drift versus baselines or time windows, slices, custom metrics, and monitoring of **inference tables** for ML/AI apps.
- Surfaced in Governance Hub as the percentage of healthy tables.

## 14. Auditing and System Tables

Every UC action is audited, queryable through **system tables**:

| System table | Contents |
|---|---|
| `system.access.audit` | Audit events (`service_name = 'unityCatalog'`; actions like `createPolicy`, `deletePolicy`, `createEntityTagAssignment`) |
| `system.access.table_lineage`, `column_lineage` | Lineage (1-year rolling) |
| `system.access.inbound_network` | Denials/dry-run results from inbound networking controls |
| `system.billing.usage` | Billing, including `billing_origin_product` values such as `DATA_CLASSIFICATION`, `DATA_QUALITY_MONITORING`, `JOBS`, `DLT` |
| `system.data_classification.results` | Classification results |
| `system.information_schema` / per-catalog `information_schema` | Metadata views |

```sql
SELECT event_time, action_name, user_identity.email AS actor, request_params.name AS policy_name
FROM system.access.audit
WHERE service_name = 'unityCatalog' AND action_name IN ('createPolicy','deletePolicy')
ORDER BY event_time DESC;
```

## 15. Data Sharing: OpenSharing and Clean Rooms

Delta Sharing has evolved into **OpenSharing**, an open protocol for sharing live data and AI assets without copying it, across organizations, clouds and platforms.
- Objects: **share** (logical group of tables, views, volumes), **recipient** (created in the provider's metastore), **provider** (created in the recipient's metastore). `SELECT` on a share is granted to a recipient.
- **Cross-region/cloud:** use Databricks-to-Databricks sharing — **you pay egress charges**, so share infrequently accessed data and see the egress-management guidance for hot data. Lineage and access control **do not cross** metastore boundaries; privileges must be re-granted on the destination.
- **Clean rooms:** privacy-safe collaboration where parties run mutually approved code without exposing raw data (`CREATE CLEAN ROOM`, `EXECUTE CLEAN ROOM TASK`, `MODIFY CLEAN ROOM`).
- Related ABAC rules: see §8.1 (OpenSharing and views/tables with policies).
- See the separate Delta Sharing / Clean Rooms guide for depth.

```sql
CREATE SHARE sales_share;
ALTER SHARE sales_share ADD TABLE production.sales.orders;
CREATE RECIPIENT partner_org USING ID 'partner-sharing-identifier';
GRANT SELECT ON SHARE sales_share TO RECIPIENT partner_org;
```

## 16. Lakehouse Federation and Connections

A **connection** stores an endpoint and credentials for an external system and supports: **query federation**, **catalog federation**, **managed ingestion**, **JDBC access** and **HTTP services**. Federated tables appear as **foreign tables** in a **foreign catalog**: read-only and governed by the normal UC privilege model.
```sql
CREATE CONNECTION pg_conn TYPE postgresql
OPTIONS (host 'db.example.com', port '5432', user 'reader', password secret('scope','pg_pw'));
CREATE FOREIGN CATALOG pg_catalog USING CONNECTION pg_conn OPTIONS (database 'analytics');
```
`CREATE CONNECTION` on the metastore is required (plus `CREATE CONNECTION` on the service credential if the connection uses one); `USE CONNECTION` lets others use it.

## 17. Open-Source Unity Catalog

Unity Catalog is also available as a standalone **open-source implementation** (public GitHub repo) with open APIs, usable outside Databricks. Don't confuse it with the Databricks-managed service described in this guide, which adds ABAC, lineage, classification, auditing, sharing and more.

## 18. AI Governance (Unity Gateway)

UC governs AI assets as securables (models, features, services, secrets). **Runtime** controls for AI — routing model traffic, guardrails via **service policies**, monitoring AI usage and cost — are covered by **Unity Gateway / AI Gateway**. Services (Beta) let you manage access to AI traffic with the same privileges used for data (`EXECUTE`), and GRANT policies can scope that access by tag (e.g. every Anthropic-hosted model via `ai.model_creator`).

## 19. Migrating from the Hive Metastore

```sql
SYNC TABLE production.sales.orders FROM hive_metastore.sales.orders;
SYNC SCHEMA production.sales FROM hive_metastore.sales;
```
- External tables register in place (no data movement) and can be migrated to managed tables later; managed Hive tables are cloned/copied into UC-managed storage.
- Pre-Nov-2023 workspaces need the upgrade path; metastores created during the **2022 public preview** (before Aug 25, 2022) may need the **privilege model upgrade** to get inheritance.
- Migrate schema by schema, run both in parallel briefly, cut consumers over, then retire Hive references.

## 20. Best Practices (from the official guidance)

**Identities**
- Provision users/groups/service principals at the **account level** via your IdP (automatic identity management, or SCIM). Avoid workspace-level SCIM and workspace-local groups.
- **Grant to groups, not users**, and use groups for ownership. Run production jobs with **service principals**.

**Privileges and admin**
- Be sparing with `ALL PRIVILEGES`, `MANAGE`, ownership and metastore admins; assign them to groups.
- Reserve direct `MODIFY` on production tables for service principals.
- Use `USE CATALOG`/`USE SCHEMA` deliberately: they stop table creators from exposing data to unapproved users.

**Structure**
- Catalogs per environment/team/business unit; bind catalogs to workspaces only when both data and environments need isolation.
- Catalog-level managed storage as the isolation unit; managed tables and volumes by default.
- Use one metastore per region for dev, test, prod and all business units.

**Fine-grained control**
- Prefer **ABAC** over per-table filters; attach policies at the highest applicable level; use groups in `TO`/`EXCEPT`; design a small, stable governed-tag taxonomy and use inheritance for safe defaults.

**Compute**
- Only **standard** and **dedicated** access modes (and all serverless/DBSQL) can read UC data. Databricks recommends **standard access mode** for all workloads; use compute policies to enforce.

**REST API usage**
- Paginate list calls (`max_results=0` and follow `next_page_token`), retry `429` with exponential backoff and jitter, and trim responses with `omit_properties`/`omit_columns`. The SDKs and CLI do this automatically.

## 21. Quick Reference

| Concept | Key point |
|---|---|
| Namespace | `catalog.schema.object`; metastore = one per region |
| Access formula | `USE CATALOG` + `USE SCHEMA` + object privilege |
| Inheritance | Catalog/schema grants inherit down; metastore grants don't; ownership doesn't |
| `MANAGE` | Grant/transfer/drop without owning; excluded from `ALL PRIVILEGES` |
| `BROWSE` | Metadata + request access, no data |
| Managed table | Delta/Iceberg; UC controls files; drop → deleted after 8 days |
| External table/volume | You control files; needs an external location |
| ABAC policy | `CREATE POLICY … TO … [EXCEPT …] FOR TABLES … MATCH COLUMNS … ON COLUMN … USING COLUMNS (…)` |
| GRANT policy | Tag-scoped grants on AI assets only |
| DENY policy (Beta) | Tag-scoped denial of `MANAGE ACCESS CONTROL`; overrides grants; metastore admins exempt |
| Access requests (Preview) | Destinations: email, Slack, Teams, webhook, redirect URL; resolution via owner → parents → metastore |
| Admin roles | Account admin (required), workspace admin (required), metastore admin (optional) |
| Governed tag | Account-level, tag-policy-enforced; ≤1,000 tags, ≤500 values |
| Lineage | Automatic, column-level; `system.access.*_lineage` (1 year) |
| Classification | AI-based `class.*` tagging; results in `system.data_classification.results` |
| Quality monitoring | Anomaly detection (freshness, completeness) + data profiling |
| Sharing | OpenSharing (share → recipient/provider); clean rooms for no-raw-data collaboration |
