# Unity Catalog: Workspace Bindings — Notes

## Overview

In Unity Catalog, objects attached to a metastore are accessible from **all workspaces** attached to that metastore by default. **Workspace binding** overrides this default, restricting an object so only specified workspaces can access it. Access from an unbound workspace is denied, even for users with explicit privilege grants on the object.

Bindings exist to support:
- Environment isolation (prod vs. dev/test)
- Preventing certain data domains from being joined together
- Compliance requirements (data must stay in designated environments/regions)

Bindings are enforced platform-wide — information schema queries, Catalog Explorer, and data lineage all only show/return objects bound to the current workspace.

---

## 1. Catalog Binding

**What it restricts:** which workspaces can see/use a given catalog.

- By default, a new catalog is accessible from every workspace attached to the metastore.
- Binding limits it to one or more specific workspaces.
- Unbound workspaces: catalog is grayed out / invisible, no child objects queryable (metastore admins and catalog owners can still see it).
- Access levels: `READ_WRITE` or `READ_ONLY` per workspace.
- **Note:** each workspace's own default "workspace catalog" is bound only to itself by default. If you unbind it or extend it to other workspaces, you must grant permissions manually — the workspace admins group is workspace-local and doesn't carry over.

**Configure via:** Catalog Explorer (catalog → Workspaces tab → "Assign to workspaces"), Databricks CLI (`databricks catalogs update` / bindings API), or Terraform (`databricks_workspace_binding`).

**Permissions required:** Metastore admin, catalog owner, or `MANAGE` + `USE CATALOG` on the catalog.

---

## 2. External Location Binding

**What it restricts:** which workspaces can access a specific cloud storage path (the external location object, which pairs a storage credential with a path).

- Useful when a storage path should only be reachable from a designated/approved workspace (e.g., a regulated data lake path restricted to a compliance workspace).
- Configured the same general way — assign the external location to specific workspaces.

**Configure via:** Catalog Explorer (external location → Workspaces tab) or CLI/API/Terraform equivalents.

---

## 3. Storage Credential Binding

**What it restricts:** which workspaces can *use* a given storage credential (the underlying cloud auth mechanism — managed identity or service principal).

- Also called "storage credential isolation."
- Typical use case: a cloud admin configures a storage credential using production cloud account credentials, and wants to ensure it's only used to create external locations from the production workspace.
- **Important timing nuance:** the binding is checked only **at the moment an external location is created** using that credential. Once the external location exists, it functions independently — later changes to the storage credential's bindings don't retroactively affect it.

**Configure via:** Catalog Explorer (storage credential → Workspaces tab → assign workspaces) or CLI/API/Terraform.

---

## 4. Service Credential Binding

**What it restricts:** which workspaces can use a specific service credential (used for authenticating to cloud services, as opposed to storage — e.g., calling cloud-native services from within Databricks).

- Same binding mechanism/philosophy as storage credentials, applied to service credentials instead.
- Optional/less commonly configured than the other three, but available for the same isolation reasons.

---

## How the Bindings Layer Together

Storage credential → external location → catalog form a chain, and **each binding is evaluated independently** — binding one layer does not automatically bind another:

```
Storage Credential (bound to Workspace A)
        ↓ used by
External Location (independently bound to Workspace A)
        ↓ used as managed storage for
Catalog (independently bound to Workspace A)
```

To fully isolate an environment end-to-end (e.g., "prod storage → prod external location → prod catalog"), each object must be deliberately bound — there's no automatic cascade.

---

## Quick Reference Table

| Binding Type         | Restricts Access To                          | Checked When...                                  |
|----------------------|-----------------------------------------------|---------------------------------------------------|
| Catalog              | A catalog and its contents                    | Any query/access against the catalog              |
| External Location    | A cloud storage path                          | Any access against the external location          |
| Storage Credential   | Use of a cloud auth credential                | At the moment an external location is created     |
| Service Credential   | Use of a cloud service auth credential        | When the service credential is used               |