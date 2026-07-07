### 1. What are Service Credentials?

A **Service Credential** is a securable object in Unity Catalog that encapsulates a long-term cloud credential (such as an AWS IAM Role) used to authenticate and grant access to **external cloud services** (e.g., AWS Secrets Manager, AWS Glue, DynamoDB, or external S3 buckets accessed programmatically).

#### Key Benefits

* **Alternative to Instance Profiles:** Unlike legacy instance profiles, service credentials are not statically tied to specific compute resources. Instead, they are mapped to Unity Catalog identities (users, groups, or service principals).
* **Enhanced Governance:** Access control is managed strictly using standard Unity Catalog SQL privileges (`GRANT` / `REVOKE`) and tracked securely via Databricks audit logs.
* **No Hardcoded Secrets:** Eliminates the security risk of hardcoding AWS keys inside notebooks, scripts, or storing them insecurely inside workspace secrets.

#### Service Credentials vs. Storage Credentials

* **Storage Credentials:** Explicitly used to back *External Locations* and manage Unity Catalog's managed tables or volumes.
* **Service Credentials:** Used exclusively for standard application or third-party SDK calls (like `boto3`) to non-storage cloud services or independent programmatic tasks.

<br></br>

### 2. Requirements & Prerequisites

To successfully deploy and utilize service credentials, your environment must satisfy the following parameters:

**Databricks Environment:**
  * A Databricks workspace enabled for **Unity Catalog**.
  * The `CREATE SERVICE CREDENTIAL` privilege on the Unity Catalog metastore. (Account admins and metastore admins have this by default. If auto-enabled for UC, workspace admins have this as well).


**AWS Environment:**
  * An AWS service situated in the same region as your Databricks workspace.
  * AWS IAM permissions capable of creating and modifying IAM roles and trust policies.


**Compute Restrictions:**
  * Databricks Runtime **16.2 or above** is recommended for full multi-language support.
  * **SQL Warehouses** and **Serverless compute** do not support native integration with service credentials (except within Batch Unity Catalog Python UDFs).

<br></br>

### 3. Creating a Service Credential

Setting up a service credential requires a two-step handshake between AWS IAM and Databricks.

#### Step 1: Create an AWS IAM Role

In your AWS Console or via Infrastructure as Code (IaC), establish an IAM Role that grants permissions to your specific cloud service (e.g., AWS Secrets Manager).
The role must include a **Trust Relationship** policy that allows Unity Catalog to assume it also should be self assuming. You can fetch or configure the trust policy to validate the Databricks account environment.

#### Step 2: Register the Service Credential in Databricks

You can create the credential asset utilizing Catalog Explorer, SQL commands, the Databricks CLI, or Terraform.

#### Method A: Using SQL

Run the following statement inside a Databricks Notebook:

```sql
CREATE SERVICE CREDENTIAL my_secret_manager_cred
  FOR IAM_ROLE 'arn:aws:iam::123456789012:role/my-aws-secrets-manager-role'
  COMMENT 'Service credential to access AWS Secrets Manager safely';

```
#### Method B: Using Databricks CLI (v0.200+)

```bash
databricks credentials create-credential my_secret_manager_cred \
  --purpose SERVICE \
  --comment "Service credential to access AWS Secrets Manager safely" \
  --json '{"aws_iam_role": {"role_arn": "arn:aws:iam::123456789012:role/my-aws-secrets-manager-role"}}'

```
#### Method C: Using Terraform

```hcl
resource "databricks_credential" "service_cred" {
  name    = "my_secret_manager_cred"
  purpose = "SERVICE"
  comment = "Managed by Terraform"
  
  aws_iam_role {
    role_arn = "arn:aws:iam::123456789012:role/my-aws-secrets-manager-role"
  }
}

```
<br></br>
### 4. Managing Service Credentials & Access Control

Once the asset is created, the owner or a metastore admin must grant explicitly mapped permissions.

#### Listing and Viewing Credentials

To view the available service credentials assigned to you, execute:

```sql
SHOW SERVICE CREDENTIALS;
DESCRIBE SERVICE CREDENTIAL my_secret_manager_cred;
```
#### Granting Privileges

To allow a user, group, or service principal to invoke this credential, grant them the `ACCESS` privilege:

**Using SQL:**

```sql
GRANT ACCESS ON SERVICE CREDENTIAL my_secret_manager_cred TO `finance_team_group@company.com`;
```
**Using Catalog Explorer:**

1. In the sidebar, navigate to **Catalog**.
2. Click **External Data** > go to the **Credentials** tab.
3. Select your credential, click **Permissions**, and choose **Grant**.
4. Select the target Identity, check **ACCESS**, and click **Grant**.

#### Renaming and Deleting Credentials
Only the credential owner or a metastore admin can perform destructive updates:
```sql
-- Rename
ALTER SERVICE CREDENTIAL my_secret_manager_cred RENAME TO backup_secret_manager_cred;
-- Delete
DROP SERVICE CREDENTIAL backup_secret_manager_cred;
```
<br></br>
### 5. Utilizing Service Credentials in Code

To utilize these configurations in a live runtime session, you fetch an abstracted credential token using the native `dbutils` library handler.

**Python Example: Authenticating `boto3`**

```python
import boto3

# Securely extract the AWS credentials provider from Unity Catalog
credential_provider = dbutils.credentials.getServiceCredentialsProvider("my_secret_manager_cred")

# Inject the provider configuration block into your boto3 Session context
boto3_session = boto3.Session(
    botocore_session=credential_provider,
    region_name="us-east-1" # Replace with your target AWS Region
)

# Open an authorized client session
sm_client = boto3_session.client("secretsmanager")
response = sm_client.list_secrets()
print(response)

```
**Usage inside User Defined Functions (UDFs**)

If referencing service credentials inside a specific execution context like a Python UDF, you must call a dedicated alternate UDF API namespace since `dbutils` will not resolve natively inside an isolated UDF worker task environment:

```python
# Inside Scalar or Batch Python UDFs:
import databricks.service_credentials

provider = databricks.service_credentials.getServiceCredentialsProvider("my_secret_manager_cred")

```
<br></br>
### 6. Specifying a Default Service Credential for Compute

If you wish to make code entirely portable without specifying the exact credential name inside the application script, you can declare a default credential context at the cluster layer.

1. Go to the cluster configuration edit view page.
2. Under **Advanced Options**, click the **Spark** tab.
3. Set the following indicator flag inside the **Environment Variables** prompt block:
```env
DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME=my_secret_manager_cred
```
