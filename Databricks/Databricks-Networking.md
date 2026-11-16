## 1. The Big Picture: Two Planes, Three Connection Points

Databricks runs out of a **control plane** and a **compute plane**:

- The **control plane** is the set of backend services Databricks manages (web application, REST APIs, job scheduler, cluster manager, Unity Catalog metadata).
- The **compute plane** is where your data is processed. There are two kinds:
  - **Classic compute plane** — compute runs in *your* AWS account, in your VPC, in the same region as the workspace.
  - **Serverless compute plane** — compute runs in a Databricks-managed account, in the same cloud region as your workspace.

Networking is easiest to reason about as **three connection points, each secured independently**:

```
 (1) Users & apps ───────────────► Control plane          "Inbound"
 (2) Control plane ◄──────────────► Classic compute plane  "Classic"  (your VPC)
 (3) Serverless compute plane ────► Your resources         "Outbound" (S3, DBs, APIs…)
```

| Connection | What you control | Main features |
|---|---|---|
| 1. Users → Databricks | Who can reach workspaces and the account console | Inbound PrivateLink, context-based ingress, IP access lists, firewall domain rules |
| 2. Control plane ↔ classic compute | How your VPC connects to Databricks | Secure cluster connectivity, customer-managed VPC, back-end PrivateLink, VPC peering |
| 3. Serverless compute → your resources | How Databricks-managed compute reaches your data | Network connectivity configurations (NCC), private endpoint rules, serverless egress control, firewall allowlisting |

Connectivity between the control plane and the serverless compute plane is always over the cloud provider's backbone, never the public internet. Your data does not pass through the control plane for query execution — only metadata, commands and results do.

## 2. Secure Cluster Connectivity (SCC)

SCC is on by default for all new workspaces:

- Your VPC has **no open inbound ports**.
- Classic compute nodes have **no public IP addresses**.
- Cluster nodes open an **outbound HTTPS (443) tunnel** to the control plane through the **SCC relay**. The control plane sends commands back down that tunnel.
- The relay separates web-app/REST-API traffic from cluster-management traffic.

Serverless compute does not use the SCC relay (it's a different architecture) but its resources also have no public IPs.

**Practical implication:** you never open an inbound rule "so Databricks can reach the cluster." The connection direction is the other way. SCC is also a prerequisite for classic back-end PrivateLink.

## 3. Non-Network Prerequisites (Cross-Account Role and Root Bucket)

Every classic AWS deployment also needs two non-network objects that govern *who may act on your behalf*:

- **Cross-account IAM role** — Databricks assumes it to launch and manage EC2 and related resources in your account. One role can be shared by multiple workspaces. With a customer-managed VPC, Databricks needs *fewer* permissions on this role than with the default setup, which often simplifies internal approvals.
- **Root S3 bucket** (with the required bucket policy) — stores workspace assets such as cluster logs, notebook revisions and job results. One bucket can be shared across workspaces.

Both can be created manually (Terraform/CloudFormation) or via **automated configuration**, where an AWS admin delegates temporary access so Databricks creates the role, bucket and (optionally) a customer-managed VPC for you.

> **Policy note:** Databricks plans to deprecate classic workspace creation with a Databricks-managed VPC; **new classic workspaces will require a customer-managed VPC**. The simplest quick start is a **serverless workspace**, which has no classic compute plane (and therefore no VPC for you to manage).

## 4. Customer-Managed VPC

### 4.1 Managed vs. customer-managed

| | Databricks-managed VPC | Customer-managed VPC |
|---|---|---|
| Who creates it | Databricks, at workspace creation | You, before creating the workspace |
| CIDR / subnet / routing control | Limited (default is a /16) | Full — smaller subnets possible |
| Back-end & inbound PrivateLink | **Not supported** for classic workspaces | **Required** prerequisite |
| Egress firewall / proxy appliance | Not practical | Supported — limit outbound traffic to an allow list |
| Cross-account role permissions | More | Fewer |
| Several workspaces share one VPC | No | Yes (recommended: unique subnets and security groups per workspace) |
| VPC peering to other VPCs | Possible, but can be complex | Possible; often avoidable via smaller subnets and shared VPCs |

You must choose at workspace creation — **you cannot convert** an existing managed-VPC workspace to a customer-managed one. You *can* later move a customer-managed workspace from one VPC to another by updating its network configuration object.

Serverless-only workspaces and outbound (serverless) PrivateLink do **not** need a customer-managed VPC.

### 4.2 VPC requirements (from the official page)

- **Region:** same region as the workspace (check the regional-availability list).
- **DNS:** `enableDnsHostnames` *and* `enableDnsResolution` must be on. Required for PrivateLink; enable from day one.
- **Sizing:** Databricks assigns **two IPs per node** (one for management, one for Spark). The number of instances per subnet = **half the available IPs**.
- **Subnet netmask:** each workspace subnet must be between **/17 and /26**. The VPC itself isn't limited, but must be large enough (two /26 subnets means the VPC is /25 or larger). For PrivateLink the VPC CIDR must be **at least /25**.
- **Subnets:** at least **two, in different Availability Zones**; only **one subnet per AZ** can be given to a workspace. Subnets must be **private**. If you have secondary CIDR blocks, workspace subnets must come from the same VPC CIDR block.
- **Egress path:** subnets need outbound access to the public network via a **NAT gateway + internet gateway**, or a customer-managed appliance. The NAT gateway sits in **its own subnet** that routes `0.0.0.0/0` to the internet gateway; the workspace subnets' route table sends `0.0.0.0/0` to the NAT gateway (or your proxy/firewall).
- **Security groups:** **1 to 5** per workspace. Required rules:
  - *Ingress:* allow all TCP and all UDP **from the same security group** (self-referencing).
  - *Egress:* allow all TCP and UDP to the workspace security group itself, plus TCP to `0.0.0.0/0` on these ports:

| Port | Purpose |
|---|---|
| 443 | Databricks infrastructure, cloud data sources, library repositories |
| 3306 | Legacy Hive metastore |
| 53 | DNS, when using custom DNS |
| 6666 | Secure cluster connectivity — **only if using PrivateLink** |
| 2443 | FIPS endpoints — **only with the compliance security profile** |
| 5432 | Classic compute plane → Lakebase |
| 8443, 8445 | Internal calls from compute plane to control-plane API |
| 8444 | Unity Catalog logging and lineage streaming |
| 8446–8451 | Reserved for future use |

- **Subnet NACLs:** must **not deny** any traffic. Databricks validates at workspace creation: inbound `ALLOW ALL from 0.0.0.0/0` (must be prioritized), outbound allow to the VPC CIDR and TCP on 443, 3306, 6666 (PrivateLink), 8443, 8445, 8444. If you add other rules, give Databricks' rules the lowest rule numbers so they win. Restriction is meant to happen at the security-group and firewall layers, not the NACL.
- **Don't over-restrict egress by IP:** if you use IP access lists, the NAT/egress public IPs must be on the allow list.

### 4.3 Regional VPC endpoints (recommended)

A customer-managed-VPC workspace must reach four AWS services: **S3, STS, Kinesis, and RDS** (RDS only for the legacy Hive metastore — not Unity Catalog — and there's no VPC endpoint for it). For the other three, create regional endpoints so in-region traffic stays on the AWS backbone, lowering cost and latency:

- **S3** — a **gateway** endpoint attached to the route table(s) of the workspace subnets. Cross-region buckets require allowing the S3 global URL or a route to an internet gateway.
- **STS** — an **interface** endpoint in the workspace subnets (use the workspace security group).
- **Kinesis** — an **interface** endpoint, same pattern. It carries **Databricks' own log/telemetry delivery**, not your workloads' data. Exception: workspaces in `us-west-1`, whose Kinesis streams are cross-region.

If endpoints don't take effect, set `AWS_REGION` on clusters (cluster policy) and/or the Spark `fs.s3a.endpoint` / `fs.s3a.stsAssumeRole.stsEndpoint` regional settings. Caution: applying the S3 regional Spark config at notebook/cluster level **blocks cross-region S3 access**.

### 4.4 Egress firewall / proxy

Databricks expects you to use an **egress firewall or proxy appliance** to block most traffic while allowing what Databricks needs. Specify destinations **by domain name, not IP**. Required allows include the Databricks web app, the SCC relay, S3 global (`s3.amazonaws.com:443`, root bucket), STS global and regional, the regional Kinesis URL, the legacy-Hive-metastore RDS URL (if used), and control-plane infrastructure. The region-specific values live on the "IP addresses and domains" reference page.

### 4.5 Restricting S3 buckets (optional)

Most S3 traffic is compute-plane-local, but some management operations originate in the control plane. To lock a bucket to known sources, use a **Deny** bucket policy (or a VPC endpoint policy). It must allow:
- the **control-plane NAT IP and VPC IDs** for your region,
- the compute plane via **`aws:sourceVpce`** (recommended) or its NAT IP,
- with VPC endpoint policies: your **root bucket** plus the regional artifact/log/system-tables/shared-datasets buckets,
- at least one **trusted IP (e.g. corporate VPN)** — Deny conditions apply even in the AWS console.

When deploying a *new* workspace into a restricted bucket, you must temporarily allow a `us-west` control-plane NAT IP or deployment fails.

### 4.6 Registering and changing the VPC

- You register the VPC (ID, subnet IDs, security group IDs) as a **network configuration** in the account console/API.
- A network configuration object **cannot be reused across workspaces** and, once created, **cannot be edited**.
- Changing subnet CIDRs means: terminate clusters → delete subnets → recreate → fix route-table associations (a missed association makes clusters fail to launch) → create a new network configuration → update the workspace.

```hcl
# Terraform skeleton — customer-managed VPC (sizes are illustrative: size for peak concurrent nodes)
resource "aws_vpc" "databricks" {
  cidr_block           = "10.100.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.databricks.id
  cidr_block        = "10.100.0.0/19"   # allowed range is /17–/26
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
  # For back-end PrivateLink, also reference the two registered VPC endpoints here
}

# Separately required: cross-account IAM role + root S3 bucket (see §3),
# referenced via databricks_mws_credentials and databricks_mws_storage_configurations.
```

## 5. VPC Peering (Reaching Your Other AWS Resources)

By default, classic compute is walled off: cluster workers talk only to workers of the **same cluster** and cannot freely reach other EC2 instances or AWS services running in the Databricks VPC. To reach things like RDS, Redshift, Kafka or Cassandra, either run them **outside** the Databricks VPC and **peer**, or (with a customer-managed VPC) allow it through your own security-group design.

Peering checklist (from the official page):
1. The other VPC's CIDR must be **distinct** from the Databricks VPC and every destination already in its route table.
2. Create the peering connection and **accept** it (cross-account needs the owner's approval).
3. Enable **DNS resolution** on the peering connection **in both accounts**.
4. Add routes to the **other VPC's CIDR** in the Databricks route table, **and** to the Databricks CIDR in the other VPC's route table.
5. Add an **inbound rule** on the target's security group (e.g. 3306 for MySQL/Aurora) with the **Databricks worker security group** as source.
6. Test from a cluster: `%sh nc -zv <host> <port>`; if the name doesn't resolve, check peering DNS settings and Route 53 private hosted zone associations.

For many-to-many topologies, a Transit Gateway is the usual AWS alternative (standard AWS practice; not detailed on the Databricks pages).

## 6. Classic (Back-End) PrivateLink

Back-end PrivateLink keeps **classic compute ↔ control plane** traffic off the public internet.

**Requirements:** Enterprise pricing tier; a **customer-managed VPC**; **secure cluster connectivity**; VPC with DNS hostnames/resolution enabled and a CIDR of at least /25; AWS permissions to create VPC endpoints.

**Two VPC interface endpoints are needed** (both in the workspace region, created from the Databricks regional service names, with *Enable private DNS name* on):
1. **Workspace endpoint** — Databricks REST APIs.
2. **SCC relay endpoint** — cluster management tunnel (port **6666**; add **2443** with the compliance security profile).

**Recommended design:**
- A **dedicated subnet** for VPC endpoints with its own route table (only the local route).
- **Two security groups:** the workspace SG, and an endpoint SG that allows inbound on the required ports **from the workspace SG** (no outbound rules needed; security groups are stateful).
- VPC endpoints can be **shared across workspaces** that use the same customer-managed VPC.

**Databricks-side objects (registration order matters):**
- **VPC endpoint registrations** — register each AWS endpoint with Databricks (can't be updated later).
- **Network configuration** — references the VPC plus the two back-end registrations (can't be updated later).
- **Private access settings (PAS)** — region-bound; one PAS can serve multiple workspaces in the same region.
- Create/update the **workspace** with the network configuration and PAS. After status shows `RUNNING`, **wait about 20 minutes** before launching clusters.

**Connecting to S3/STS/Kinesis — two options:**
- **Option 1 (standard):** keep NAT + internet gateway, with optional S3/STS/Kinesis endpoints.
- **Option 2 (fully private / air-gapped):** *remove* NAT and internet gateway; S3 (gateway), STS and Kinesis (interface) endpoints become **mandatory**. Use a private subnet of at least /27 for the interface endpoints and an SG allowing 443 from the workspace SG.

**Verify** from a notebook: call a workspace REST API with a PAT, and run `%sh nslookup <workspace-url>` — the answer must be the **private IP** of your endpoint. A public answer means DNS is misconfigured even if the call succeeds.

## 7. Inbound Networking: Users → Databricks

Features here require the Enterprise plan.

### 7.1 Inbound (front-end) PrivateLink

- Routes users' access to the web app, REST APIs, Databricks Apps and Databricks Connect through a **VPC interface endpoint**, usually in a **transit VPC** (often separate from the compute VPC, and reachable from on-premises via Direct Connect or VPN; TCP 443 must be allowed from client networks).
- You create **one endpoint type: "General Access (including REST API)"**, register it in the account console (*Security → Private endpoints*), and attach a policy. A General Access endpoint in **any region** can serve workspaces in **any region**, and one endpoint can be shared across Databricks accounts by registering it in each.
- Classic workspaces need a **customer-managed VPC** for inbound PrivateLink.

> **Correction to earlier guidance:** current docs describe a single *General Access* endpoint for inbound access; there is no separate "browser authentication endpoint" in the current model. For users without public-internet access, SSO via **unified login** redirects to `accounts.cloud.databricks.com`, which needs an extra IdP redirect URI and a private hosted zone (private access to account-level resources is in **Beta**).

### 7.2 Deciding who may use the endpoint

Two mechanisms, both of which must allow the endpoint:
- **Private access settings (PAS)** — all-or-nothing, region-bound.
- **Context-based ingress (CBI)** — fine-grained policies combining identity, request type and network source (public IP ranges or specific private endpoints); not region-bound. Default is *Allow access from all private endpoints*, deferring to PAS. If you use CBI, attach an "allow all" PAS so CBI makes the decision. Workspaces with **no PAS attached deny all private access**.
- Test in **Dry run mode** first; denials are logged to `system.access.inbound_network` but don't block. Changes take under ~10 minutes. One policy per workspace, but a policy can be shared by many.

### 7.3 DNS

Every workspace has two URLs: the **workspace-specific** URL (`dbc-…cloud.databricks.com`) — **always** route it to the endpoint — and your **custom** URL, which you must also route if you use it. Verify with `nslookup`: the workspace-specific URL should show a **`privatelink`** canonical name and a private IP; if not, no PAS is attached. Workspaces with **managed disaster recovery** use the custom URL as a stable URL and need a failover General Access endpoint allowlisted to both primary and secondary workspaces.

### 7.4 IP access lists

- Allow/block lists on the **public** source IP of requests, for **workspaces** (REST API) and the **account console**. Block lists are evaluated first; with no allow list, everything is allowed; account console max **1000** values; changes take a few minutes.
- **IP access lists apply only to public-internet requests.** They cannot block private IPs arriving over PrivateLink — use AWS Network Firewall, a PAS `ENDPOINT` access level, or CBI "Selected private endpoints" for that.
- **Both controls apply together** with CBI. Databricks recommends CBI as the primary mechanism and provides a migration path from IP access lists.

### 7.5 Domain firewalls

If your organization blocks by domain, allowlist the Databricks domains. Databricks also validates the **Host header** and rejects requests for non-Databricks domains.

## 8. Reference Architectures (Three Tiers)

Start at Managed security and add controls as requirements grow. Most organizations harden ingress and egress before going fully private.

| Feature | Managed security | Hardened connectivity | Isolated environment |
|---|---|---|---|
| Secure cluster connectivity | ✅ | ✅ | ✅ |
| Customer-managed VPC | ✅ | ✅ | ✅ |
| Classic (back-end) PrivateLink | Optional | ✅ | ✅ |
| Workspace inbound PrivateLink | ❌ | ❌ | ✅ |
| Inbound PrivateLink for performance-intensive services | ❌ | ❌ | ✅ |
| Workspace / account / OpenSharing IP access lists | ❌ | ✅ | ✅ |
| Serverless egress control | ❌ | ✅ | ✅ |
| Serverless PrivateLink (NCC private endpoints) | ❌ | ✅ | ✅ |
| Serverless firewall allowlisting (stable IPs) | ✅ | ✅ | ✅ |
| External firewall | Optional | Optional | ✅ (required) |

- **Managed security** — secure defaults; apply Unity Catalog governance on top.
- **Hardened connectivity** — auditability and access control *without* eliminating public endpoints.
- **Isolated environment** — everything private; for regulated industries with strict exfiltration requirements.

## 9. Serverless Compute Plane Networking

Serverless runs in the Databricks-managed serverless plane, so your VPC settings don't apply. Instead there are **network connectivity configurations (NCCs)** and **network policies**.

### 9.1 NCCs

- An **account-level, regional** object that manages **private endpoint creation** and rules.
- Attach one NCC to **many workspaces in the same region**; all its private endpoint rules apply to them.
- Limits: **10 NCCs per region per account**, **30 private endpoints per region** (spread across NCCs), **50 workspaces per NCC**, **100 domain names per private endpoint rule** (VPC-resource rules). No DNS chasing/redirects: names must resolve directly to the backend.
- An NCC can also be attached to a **metastore** for **OpenSharing SecureConnect** private connectivity.

### 9.2 Firewalling your resources for serverless (the stable-IP change)

> **Correction:** earlier guidance described "stable IPs" as an NCC feature. The **legacy NCC stable-IP list was deprecated (May 25, 2026)** and removed from the NCC UI/API. Still using it can cause `403 Forbidden` from serverless.

Current method depends on the resource:
- **S3 buckets in the workspace's region:** use the **`aws:VpceOrgPaths`** condition key in the bucket policy with the Databricks serverless OrgPath (serverless VPCs each have an S3 gateway endpoint; gateway endpoints are regional).
- **Everything else:** allowlist the published outbound CIDRs from **`ip-ranges.json`** (**Public Preview**) — filter for `service = Databricks`, `type = outbound`, your `region`, `platform = aws`. **Automate the refresh:** updates publish as often as every 30 days and new IPs go live ~60 days after publication; compare the file's `timestampSeconds`.
- Serverless connects over **IPv4 only**; firewall rules also affect classic compute, so allow its networks too.

### 9.3 Private connectivity (PrivateLink) from serverless

**To resources in your VPC (via NLB):** (Enterprise plan)
1. Internal **Network Load Balancer** in front of the resource.
2. **VPC endpoint service** on it, with *acceptance required*, *Enforce inbound rules on PrivateLink traffic* **off**, and the Databricks serverless stable IAM role (`private-connectivity-role-<region>`) — or `*` for simplicity — in *Allow principals*. Cross-region requires two AZs and adding the consumer region.
3. Create an **NCC** (same region as the workspace).
4. Add a **private endpoint rule**: endpoint service name + the destination **FQDN(s)** (e.g. the RDS hostname).
5. **Accept** the connection request in the AWS console; wait for status **`ESTABLISHED`**.
6. **Attach the NCC** to workspaces.

Domains added via private endpoint rules are **implicitly allowlisted** in network policies. RDS Multi-AZ has no stable IPs, so NLB target IPs need automated sync.

**To AWS-managed resources (Public Preview):** supported targets include **S3 (and S3 FIPS), DynamoDB, STS, Secrets Manager, KMS, Glue, Lambda, SNS, SQS, EMR, RDS APIs, Redshift (incl. Data API/Serverless), and Bedrock**.
- S3 supports per-bucket scoping (up to 100 buckets per rule, one S3 rule per region); other types apply to all traffic for that resource type in attached workspaces.
- Access S3 via the **regional** DNS name (`<bucket>.s3.<region>.amazonaws.com`); global names aren't supported.
- **Restrict the resource first, then enable the rule.** Use an explicit **Deny** unless `aws:SourceVpce` matches — but also allow admin roles/VPN, or you can lock yourself out of the console. For S3, also allow the Databricks control plane (Unity Catalog reaches the bucket from there).
- These endpoints cover **service APIs**, not direct JDBC/ODBC connections to RDS/Redshift instances.

### 9.4 Serverless egress control (network policies)

Previously missing from this guide. A **network policy** (account-level object; one per workspace, sharable across workspaces; Enterprise tier) governs outbound traffic from serverless products — notebooks, workflows, SQL warehouses, Lakeflow pipelines, Model Serving, Data Quality Monitoring, and Apps (limited support).

- **Full access** — unrestricted outbound.
- **Restricted access** — deny-by-default. Allowed: **Unity Catalog external locations** (same region as the bucket), **FQDNs and S3 buckets listed in the policy**, and the **same workspace's** APIs. Always allowed: the workspace S3 bucket, essential system tables, sample datasets.
- Direct storage access from user code (UDFs/notebooks) is prohibited in restricted mode — use Unity Catalog or DBFS mounts.
- Private-endpoint traffic is **also** subject to the policy.
- External model endpoints and external **MCP servers** need their DNS names added.
- Use **dry-run mode** before enforcing.
- **Cross-workspace access** is separate: allowed only if both the source's egress rules and destination's ingress rules permit it.

### 9.5 Private network gateway **(Private Preview, unlisted docs page)**

Connects serverless compute to your VPC and transitively connected networks (e.g. on-premises over Direct Connect/VPN) through one managed gateway — you supply a subnet, a cross-account IAM role and a security group.
- Egress paths are evaluated in priority order: **private endpoint rules → S3/DynamoDB gateway endpoints → private network gateway → default Databricks egress**.
- Two modes: **`SPECIFIC_DESTINATIONS`** (recommended; destination suffix matching) and **`ALL_TRAFFIC`** (must add destinations to the egress policy yourself).
- Limits in preview: one subnet in one AZ, **API-only configuration (no UI or Terraform)**, max two gateways per NCC, 100 destinations. Doesn't reach AWS-managed services behind gateway/interface endpoints.

## 10. Data-Access Touchpoints

### 10.1 The S3 commit service

Delta Lake, Structured Streaming, Auto Loader and `COPY INTO` use a **commit service in the control plane** to guarantee consistent writes (S3 can't do "put if absent"). Clusters write data directly to S3; the commit service finalizes the commit-log multipart upload and **never reads** your data.
- By default, temporary credentials are sent to the control plane (instance-profile credentials last six hours).
- Instance-profile users may see **GuardDuty** `InstanceCredentialExfiltration` alerts; Unity Catalog–managed tables **don't** trigger them. Mitigation: a token-based instance profile that assumes the data role.
- Optional: disable the *direct upload optimization* (`spark.hadoop.fs.s3a.databricks.s3commit.directPutFileSizeThreshold = 0`), at a small cost for tiny near-real-time streaming updates.
- Include control-plane IPs in any S3 IP-restriction policy.

### 10.2 Unity Catalog and federation

- Classic compute reaches governed buckets via the IAM role of the storage credential, ideally through an S3 gateway endpoint.
- **Lakehouse Federation / external databases:** need real reachability — private endpoint rules or published IPs from serverless; VPC peering/endpoints from classic.
- **Credential vending** to external engines relies on those clients reaching both UC and storage over your existing paths.

## 11. Networking Costs

- **Classic compute:** you pay AWS directly for networking.
- **Serverless:** Databricks charges for private connectivity (per GB; per-hour endpoint fees expected later), public connectivity (per GB), and data transfer (cross-AZ, cross-region, internet). **In-region S3 access from serverless uses gateway endpoints and is not charged.**
- **Performance-intensive services** (Lakebase, OpenSharing SecureConnect, Zerobus, Files API): cross-region and public-internet **egress back to clients** is charged for all customers. Ordinary UI/API traffic is not.
- Create workspaces in the **same region as your data** to avoid cross-region charges. Usage appears in the billable-usage system table.

## 12. Common Gotchas

- **Cluster can't reach my database** → missing peering/route on one side, overlapping CIDRs, DNS resolution not enabled on the peering connection, or the target SG not allowing the Databricks worker SG.
- **PrivateLink not working** → DNS hostnames/resolution disabled; endpoint DNS not private (`nslookup`); no PAS attached; didn't wait ~20 minutes after `RUNNING`.
- **Workspace creation / cluster launch fails** → NACL denying required ports; subnet not private or no NAT route; undersized subnet (remember two IPs per node); wrong route-table association after a CIDR change.
- **Only back-end PrivateLink configured** → users still reach the UI over the public internet; add inbound PrivateLink plus CBI/PAS and disable public access *after* DNS is verified.
- **IP access lists don't block my PrivateLink users** → by design, they only see public IPs.
- **Serverless gets `403 Forbidden` after May 2026** → still using the deprecated NCC stable-IP list; migrate to `ip-ranges.json` / VPCE OrgPath.
- **Locked out of my own S3/DynamoDB resource** → Deny policy applied without allowing admin roles/VPN.
- **Restricted egress broke an app/model** → external model providers, MCP servers and S3 DNS names must be in the network policy's allowed domains.
- **Reusing network objects** → network configurations and VPC endpoint registrations are immutable; plan to create new ones rather than edit.
- **Chose a managed VPC** → can't be converted; and managed-VPC classic workspaces will stop being creatable.

## 13. Quick Reference

| Concept | What it does |
|---|---|
| Control plane / compute planes | Databricks-managed services vs. classic (your VPC) and serverless (Databricks-managed) compute |
| SCC | No public IPs, no open inbound ports; outbound relay tunnel |
| Cross-account IAM role / root bucket | Delegated EC2 management / workspace asset storage |
| Customer-managed VPC | Your VPC; required for back-end and inbound PrivateLink on classic workspaces |
| Network configuration | Registered VPC + subnets + SGs (+ back-end endpoints); immutable, not reusable |
| Back-end PrivateLink | Workspace + SCC relay endpoints; private classic↔control-plane traffic |
| Inbound PrivateLink | General Access endpoint in a transit VPC for users |
| Private access settings (PAS) | Region-bound allow/deny for private access |
| Context-based ingress (CBI) | Identity + request type + network source policies; dry-run supported |
| IP access lists | Public-IP allow/block lists; don't affect PrivateLink traffic |
| NCC | Regional, account-level serverless networking object; hosts private endpoint rules |
| Private endpoint rule | PrivateLink from serverless to NLB-fronted VPC resources or AWS-managed services |
| Serverless egress control | Network policies: full vs. restricted (deny-by-default) access |
| VPCE OrgPath / `ip-ranges.json` | Current ways to firewall resources for serverless |
| Private network gateway (Preview) | Managed gateway from serverless into your VPC / connected networks |
| S3 commit service | Control-plane service ensuring consistent Delta/streaming writes |

---

*Sources: docs.databricks.com/aws/en/security/network/ and subpages — Networking overview; Network reference architecture overview; Classic compute plane networking; Customer-managed VPC; VPC peering; Classic (back-end) PrivateLink; Inbound PrivateLink; IP access lists; Serverless compute plane networking; Serverless firewall configuration; Private connectivity to resources in your VPC and to AWS-managed resources; Serverless egress control; Private network gateway; S3 commit service; Networking costs.*
