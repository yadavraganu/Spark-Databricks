## 1. The Two-Plane Architecture

Every Databricks deployment splits into two planes that live in different AWS accounts:

```
┌─────────────────────────────┐        ┌─────────────────────────────┐
│   CONTROL PLANE               │        │   COMPUTE / DATA PLANE        │
│   (Databricks' AWS account)   │◄──────►│   (YOUR AWS account)          │
│                                │  HTTPS │                                │
│  - Web application / UI       │  (443) │  - EC2 clusters (classic)     │
│  - Jobs scheduler              │        │  - Serverless compute plane   │
│  - Cluster manager             │        │  - Your VPC, S3 buckets       │
│  - Unity Catalog metadata      │        │  - Your data                  │
└─────────────────────────────┘        └─────────────────────────────┘
```

- The **control plane** is fully managed by Databricks — notebooks, job scheduling, the web app, cluster orchestration APIs, and (for Unity Catalog) metadata all live there.
- The **compute/data plane** is where your actual workloads run and your data lives — classic compute runs in a VPC inside your AWS account; serverless compute runs in a Databricks-managed compute plane but still processes your data without it leaving your control.
- Your data **never passes through** the control plane for query execution — only metadata, commands, and results do. This split is the foundation everything else in this guide builds on.

---

## 2. Secure Cluster Connectivity (SCC)

**Secure Cluster Connectivity** is the default-on networking posture for the classic compute plane, and it's the single most important fact to know before designing anything else:

- Your VPC has **no open inbound ports** — nothing listens for connections from the control plane.
- Classic compute nodes have **no public IP addresses**.
- Instead, cluster nodes **initiate an outbound** HTTPS (port 443) connection to the control plane through a **secure cluster connectivity relay**, which the control plane uses to send commands back down that already-open tunnel.
- The relay deliberately separates two traffic types: web-app/REST-API traffic, and cluster-management traffic — so each can be secured and rate-limited independently.

Serverless compute doesn't use the SCC relay (it's a different architecture entirely — see §7), but it shares the same outcome: no public IP addresses on serverless compute resources either.

**Practical implication:** you don't need to open any inbound firewall rules for Databricks to function. If someone's proposing an inbound rule "to let Databricks reach the cluster," that's almost always unnecessary — the connection direction is the other way.

---

## 3. Non-Network Prerequisites Worth Knowing

Before the VPC-level decisions below, every AWS deployment (regardless of VPC choice) also needs two things that are easy to forget are part of "networking" in a broad sense, because they govern *who's allowed to act on your behalf*:

- **A cross-account IAM role** in your AWS account, which Databricks assumes (via `sts:AssumeRole`) so it can launch and manage EC2 instances, EBS volumes, and other compute resources in your VPC. One role can be shared across multiple workspaces — you don't need a new one per workspace.
- **A root S3 bucket** (with a specific required bucket policy) that stores workspace assets — cluster logs, notebook revisions, job results. Like the IAM role, one root bucket can be shared across multiple workspaces in an account, with data partitioned into per-workspace directories.

Both can be provisioned manually (Terraform, CloudFormation) or Databricks can provision them for you via **automated configuration**, where an AWS admin grants Databricks temporary delegated access to create the role, bucket, and policies directly. Automated configuration is the faster path for a first workspace; manual provisioning is typical once you're managing deployments as code.

> **Policy note:** Databricks-managed VPCs are being phased out for new classic workspaces — a customer-managed VPC will soon be (or may already be, depending on when you're reading this) required for any new classic-compute workspace. Check current workspace-creation requirements before assuming a managed VPC is still an option.

---

## 4. VPC Options: Databricks-Managed vs. Customer-Managed

| | Databricks-Managed VPC | Customer-Managed VPC |
|---|---|---|
| **Who creates it** | Databricks, automatically, at workspace creation | You, before creating the workspace |
| **Control over CIDR ranges, subnets, routing** | None | Full |
| **PrivateLink support** | Not available | Required prerequisite |
| **VPC peering to your other AWS resources** | Not possible | Supported |
| **Custom security groups, NACLs, egress firewalling** | Not possible | Supported |
| **Effort** | Zero — fastest path to a working workspace | You own subnet sizing, routing, NAT, endpoints |

**You cannot convert a Databricks-managed VPC into a customer-managed one after the fact** — the choice is made at workspace creation and is effectively permanent. Any production deployment with real governance or connectivity requirements should start with a customer-managed VPC.

### Customer-managed VPC requirements
- **Region:** the VPC must be in the same AWS region as the workspace.
- **Subnets:** at least two subnets in different Availability Zones (a workspace can reference up to a handful of subnets total). Size them by expected cluster footprint, not a fixed CIDR guess — each cluster node consumes roughly two IPs, so subnet size should be driven by "how many concurrent nodes across all clusters, at peak, could this workspace run," not a one-size default. Undersized subnets are one of the most common causes of "cluster failed to launch" errors as usage grows.
- **DNS:** `enableDnsHostnames` and `enableDnsResolution` must both be enabled on the VPC — required for PrivateLink to function, so it's worth enabling from day one even if you're not using PrivateLink yet.
- **Security groups:** a workspace needs at least one, and can reference up to five. Required rules include a **self-referencing ingress rule** (allow all traffic from other resources in the same security group, so cluster nodes can talk to each other) and an egress rule allowing outbound traffic to the security group itself plus the internet (443 to the control plane, plus whatever else your workloads need to reach).
- **NACLs:** Databricks requires an "allow all" rule on the subnet-level NACLs — the actual restriction happens at the security-group layer instead. This surprises teams used to locking down NACLs directly; it's a deliberate layered-security design, not an oversight, so don't fight it by adding NACL restrictions that duplicate the security group.
- **NAT Gateway (or equivalent egress path):** needed so subnets without public IPs can still reach the internet/control plane outbound — standard pattern is private subnets routing through a NAT Gateway in a public subnet.
- **VPC endpoints (recommended, required for full isolation):** S3 (Gateway endpoint), STS, and Kinesis interface endpoints let traffic to those AWS services stay on the AWS network backbone instead of routing out through the NAT Gateway and back in over the public internet. The Kinesis endpoint specifically exists for Databricks' own internal log/telemetry delivery, not something your workloads call directly — easy to assume it's optional and skip, but Databricks lists it as a standard best-practice endpoint alongside S3 and STS.

```hcl
# Terraform — customer-managed VPC skeleton
resource "aws_vpc" "databricks" {
  cidr_block           = "10.100.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support    = true
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.databricks.id
  cidr_block        = "10.100.0.0/19"   # size for peak concurrent cluster nodes, not a fixed default
  availability_zone = "us-east-1a"
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.databricks.id
  cidr_block        = "10.100.32.0/19"
  availability_zone = "us-east-1b"
}

resource "databricks_mws_networks" "this" {
  account_id         = var.databricks_account_id
  network_name       = "databricks-network"
  vpc_id             = aws_vpc.databricks.id
  subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids = [aws_security_group.databricks.id]
}

# Separately required: cross-account IAM role + root S3 bucket (see §3),
# referenced via databricks_mws_credentials and databricks_mws_storage_configurations.
```

### Reaching your other AWS resources
Classic compute nodes **cannot talk to arbitrary EC2 instances or services sitting in the Databricks VPC** — the security group firewall rules block it by design, even if you've put something else in that same VPC. To let clusters reach resources elsewhere in your AWS footprint (an RDS database, an internal API), you need actual networking: **VPC peering** or a **Transit Gateway** connection from the Databricks VPC to the VPC where that resource lives, plus routing and security group rules on both sides.

---

## 5. AWS PrivateLink

PrivateLink lets traffic between your network and the Databricks control plane travel entirely over the AWS backbone, never touching the public internet. There are three distinct PrivateLink connection types, and it's easy to conflate them:

| Type | What it secures | Typical use |
|---|---|---|
| **Back-end PrivateLink** | Classic compute plane → control plane: REST API calls and the SCC relay | Keep cluster-management traffic off the public internet |
| **Front-end PrivateLink** | Users' browsers/API clients → workspace UI and REST API | Keep end-user access off the public internet entirely |
| **Browser authentication PrivateLink endpoint** | The SSO/login redirect flow specifically | Needed alongside front-end PrivateLink so authentication itself doesn't fall back to a public path |

**Prerequisites for any PrivateLink setup:**
- A customer-managed VPC (mandatory — PrivateLink is not available on a Databricks-managed VPC).
- Secure cluster connectivity already enabled (true by default for new workspaces).
- DNS hostnames and DNS resolution enabled on the VPC.

**Back-end PrivateLink** requires creating two separate VPC interface endpoints — one for the workspace's REST API, one for the secure cluster connectivity relay — each pointed at the correct regional PrivateLink service name for your deployment.

**Front-end PrivateLink** is what you add on top when you want to prevent the workspace URL itself from ever resolving to a public IP — commonly paired with private DNS so `*.cloud.databricks.com` for your workspace resolves privately inside your network.

### The three common architecture tiers
Databricks' own security architecture guidance frames deployments as a progression, so you can match effort to actual requirement rather than over-building:

1. **Managed security (baseline)** — secure cluster connectivity + a customer-managed VPC. No open ports, no public IPs on compute, but control-plane traffic still exits over the public internet (encrypted, but not PrivateLink-routed).
2. **Hardened connectivity** — adds context-based ingress rules (IP access lists — see §6), VPC endpoints for AWS services, and serverless egress controls.
3. **Isolated environment** — adds inbound (back-end + front-end) PrivateLink and typically a required network firewall, for full private connectivity with effectively nothing routing over the public internet.

Most organizations land on tier 2; tier 3 is for environments with genuine regulatory or contractual requirements for zero public-internet exposure.

---

## 6. IP Access Lists

A simpler, complementary control to PrivateLink: **IP access lists** restrict which public IP ranges are even allowed to reach the workspace UI/API at all — an allow/deny list evaluated before a request is let through.

- Good for restricting access to known corporate IP ranges or a VPN/bastion exit IP.
- Works independently of PrivateLink — you can use IP access lists alone, PrivateLink alone, or both together (PrivateLink traffic typically needs its own allow entry if access lists are also enforced).
- Applies at the workspace level and also affects Databricks Apps traffic, since Apps run on the same ingress path.

---

## 7. Serverless Networking

Serverless compute (serverless SQL warehouses, serverless jobs/notebooks, serverless pipelines, Databricks Apps, Model Serving) runs in a **Databricks-managed compute plane**, not in your VPC — so the customer-managed-VPC model above doesn't directly apply. Serverless has its own networking primitive: the **Network Connectivity Configuration (NCC)**.

An NCC is created once and then attached to one or more workspaces, and it controls how serverless compute reaches the outside world:

- **Stable IP addresses** — serverless egress traffic comes from a fixed, published set of public IPs you can safely allowlist on external systems (a database, an API), instead of serverless traffic appearing to come from an unpredictable address range.
- **Private endpoint rules** — configure PrivateLink connections from the serverless compute plane to specific destinations (your VPC resources, an AWS service), so serverless egress to that destination stays off the public internet too.
- **Private network gateway** (newer capability, in private preview at time of writing — confirm current availability before relying on it) — complements private endpoint rules by letting you route *all* serverless egress for a workload through your own firewall or dedicated egress path, and gives you a single, identifiable set of source IPs for that traffic, reusing the same NCC you already have rather than introducing a separate object.

```
1. Create an NCC (name, region)
2. Add private endpoint rules for specific destinations you want privately reachable
   (or configure stable-IP egress for simple IP allowlisting)
3. Attach the NCC to your workspace(s)
```

A common pattern for reaching an internal database from serverless compute: stand up a Network Load Balancer (NLB) in front of the database in your VPC, create a VPC endpoint service from it, then build the NCC's private endpoint rule against that endpoint service — effectively PrivateLink, initiated from the serverless side.

---

## 8. Unity Catalog and Networking

Unity Catalog's governance model (covered in depth separately) has a few networking-specific touchpoints worth knowing:

- **Storage access** — classic compute reaches governed S3 buckets using the IAM role tied to the storage credential, routed per the VPC's own networking (ideally via an S3 VPC endpoint, as noted in §4).
- **Lakehouse Federation** — querying an external database (Postgres, Snowflake, Redshift, etc.) through a UC `CONNECTION` needs real network reachability between Databricks compute and that database, same as any other service: stable-IP allowlisting from serverless, or PrivateLink when the external system lives on AWS and you want it kept off the public internet.
- **Credential vending** to external engines (non-Databricks Spark, DuckDB, Iceberg REST clients) similarly depends on those clients having network reachability to both the UC REST endpoint and the underlying cloud storage — it rides on whatever networking path you've already built, rather than introducing a new one.

---

## 9. Common Gotchas

- **"My cluster can't reach my RDS instance"** — almost always a missing VPC peering/Transit Gateway connection, or a security group on the RDS side that doesn't allow the Databricks VPC's CIDR range. Remember clusters can't freely reach *anything* outside their own security group rules, including other resources in the same VPC.
- **"PrivateLink setup isn't working"** — check DNS hostnames/resolution are actually enabled on the VPC first; this is the most commonly skipped prerequisite, and PrivateLink silently fails (or falls back to public resolution) without it.
- **"Workspace creation failed, subnet too small"** — classic compute subnets need real headroom for autoscaling; undersized `/26`-ish subnets work fine for a demo and then fail under real load.
- **Confusing front-end and back-end PrivateLink** — back-end alone secures cluster-to-control-plane traffic but still leaves users hitting the workspace URL over the public internet; if the requirement is "no public internet exposure at all," you need both, plus the browser authentication endpoint.
- **Serverless "stable IP" vs. classic compute IP** — these are two unrelated concepts (NCC stable IPs for serverless vs. your VPC's own NAT Gateway IP for classic); don't assume allowlisting one covers the other if your workloads use both.
- **Databricks-managed VPC chosen by default, PrivateLink needed later** — can't be fixed in place; requires creating a new workspace on a customer-managed VPC and migrating.

---

## 10. Quick Reference

| Concept | What it governs |
|---|---|
| Control plane | Databricks-managed: UI, scheduler, cluster manager, UC metadata |
| Compute/data plane | Your AWS account (classic) or Databricks-managed compute plane (serverless) |
| Secure Cluster Connectivity (SCC) | No public IPs, no open inbound ports, outbound-only relay tunnel |
| Cross-account IAM role | Lets Databricks launch/manage EC2 and related resources on your behalf |
| Root S3 bucket | Stores workspace assets: cluster logs, notebook revisions, job results |
| Databricks-managed VPC | Zero-config, no PrivateLink, no peering — being phased out for new classic workspaces |
| Customer-managed VPC | Full control; required for PrivateLink, peering, custom security groups |
| Back-end PrivateLink | Classic compute ↔ control plane (REST API + SCC relay) stays private |
| Front-end PrivateLink | User access to workspace UI/API stays private |
| Browser authentication PrivateLink | SSO/login flow stays private (pairs with front-end) |
| IP access lists | Allow/deny by public IP range, independent of PrivateLink |
| NCC (Network Connectivity Configuration) | Serverless compute's networking control: stable IPs, private endpoints, gateway |
| Private network gateway | Route all serverless egress through your own firewall/egress path |
| VPC peering / Transit Gateway | Let classic compute reach other resources in your AWS footprint |

---

*Sources: Databricks official AWS networking and security architecture documentation, including classic compute plane networking, PrivateLink, serverless network security, and Databricks Apps networking pages.*
