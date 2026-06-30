Row filters and column masks allow you to apply **Row-Level Security (RLS)** and **Column-Level Security (CLS)** dynamically.  
Because these rules evaluate at query time, they prevent information leaks without requiring you to duplicate data or alter underlying files in your lakehouse.
<br></br>
### 1. Choosing Your Governance Architecture

Unity Catalog provides three primary methods to implement row and column restrictions depending on your organizational scale:

| Governance Approach | Scope of Impact | Management Command | Best Practice Use Case |
| --- | --- | --- | --- |
| **ABAC Policies** *(Recommended)* | Dynamic across catalogs/schemas based on metadata tags | `CREATE POLICY` (Centralized Data Governance) | Enforcing structural rules across hundreds of tables automatically without manual configuration. |
| **Table-Level Controls** | Hardcoded to specific tables or individual columns | `ALTER TABLE` (Table Owners) | Fast, isolated deployments or one-off business logic rules that do not require scaling. |
| **Dynamic Views** | Virtual layer sitting over base tables | `CREATE VIEW` (Workspace Developers) | Providing clean, joined, or pre-aggregated data shapes to consumers who lack direct base table access. |

<br></br>
### 2. Row Filters (Row-Level Security)

Row filters hide specific records from unauthorized users. They execute a user-defined function (UDF) that evaluates to a Boolean value for every row. If the UDF returns `FALSE` or `NULL`, the row is omitted from the result set.

#### Table-Level Implementation Pattern

This example restricts standard users to records containing the `US` region while allowing administrators full visibility.

```sql
-- Step 1: Initialize the base schema
CREATE TABLE main.default.sales_records (
    order_id INT,
    product STRING,
    amount DOUBLE,
    region STRING
);

-- Step 2: Write the row filter logic as a SQL UDF
-- The function must return a BOOLEAN (TRUE retains the row)
CREATE FUNCTION main.default.regional_sales_filter(region_param STRING)
RETURNS BOOLEAN
RETURN 
    is_account_group_member('admin') 
    OR region_param = 'US';

-- Step 3: Explicitly apply the filter to your table target
ALTER TABLE main.default.sales_records 
SET ROW FILTER main.default.regional_sales_filter ON (region);

```
<br></br>
### 3. Column Masks (Column-Level Security)

Column masks dynamically redact, mask, or replace sensitive data inside individual columns. The masking UDF takes the original column value as an input and returns a modified value.

> **Important Constraint:** The data type returned by your masking function must precisely match or be safely castable to the underlying column's original data type.

#### Table-Level Implementation Pattern

This pattern intercepts queries targeting a Social Security Number (`ssn`) column, stripping visibility for anyone outside the `hr_dept` group.

```sql
-- Step 1: Create the target table
CREATE TABLE main.default.employee_profiles (
    emp_id INT,
    name STRING,
    ssn STRING
);

-- Step 2: Build the masking function 
CREATE FUNCTION main.default.ssn_mask_fn(ssn_param STRING)
RETURNS STRING
RETURN CASE 
    WHEN is_account_group_member('hr_dept') THEN ssn_param 
    ELSE 'XXX-XX-XXXX' 
END;

-- Step 3: Mount the mask onto the sensitive column
ALTER TABLE main.default.employee_profiles 
ALTER COLUMN ssn SET MASK main.default.ssn_mask_fn;

```
<br></br>
### 4. Advanced Governance Deployment Patterns

### Pattern A: Attribute-Based Access Control (ABAC) Policies

ABAC separates security logic from individual table management. By applying **Governed Tags** to columns or tables, a single central policy can automatically protect data anywhere those tags appear across a catalog or schema.

```sql
-- Step 1: Register a governed tag schema
CREATE GOVERNED TAG main.default.pii 
DESCRIPTION 'PII Classifications'
VALUES ('ssn', 'email', 'phone');

-- Step 2: Tag the asset column (This can be fully automated via catalog discovery)
ALTER TABLE main.default.employee_profiles 
ALTER COLUMN ssn SET TAGS (main.default.pii = 'ssn');

-- Step 3: Write the generalized mask UDF
CREATE FUNCTION main.default.abac_ssn_mask(ssn_val STRING)
RETURNS STRING
RETURN CASE 
    WHEN is_account_group_member('hr_dept') THEN ssn_val 
    ELSE 'XXX-XX-XXXX' 
END;

-- Step 4: Broadcast the policy across the entire Catalog
CREATE POLICY main.default.mask_ssn_policy
ON CATALOG main
COLUMN MASK main.default.abac_ssn_mask
TO `account users` EXCEPT `hr_admins`
FOR TABLES
MATCH COLUMNS has_tag_value('main.default.pii', 'ssn') AS target_col
ON COLUMN target_col;

```

### Pattern B: Dynamic Views

If you prefer not to touch the metadata of your underlying physical Delta tables, or if you need to merge multiple layers of masking and filtering into a clean pipeline, you can embed the security functions directly within a standard View layout.

```sql
-- Create a single Dynamic View handling both RLS and CLS natively
CREATE VIEW main.default.v_secure_employee_data AS
SELECT 
    emp_id,
    name,
    -- Column Masking integrated via View logic
    CASE 
        WHEN is_account_group_member('hr_dept') THEN salary
        ELSE 0.0  
    END AS salary,
    region
FROM main.default.raw_employee_data
WHERE 
    -- Row Filtering integrated via View logic
    is_account_group_member('admin') 
    OR region = 'US';

```
<br></br>
### 5. Deleting Governance Rules

If you need to refactor, upgrade, or remove existing data policies, table owners can strip policies using clean alternative `DROP` commands:

```sql
-- Strip a row filter policy off a target table
ALTER TABLE main.default.sales_records DROP ROW FILTER;

-- Strip a column mask policy off an individual column
ALTER TABLE main.default.employee_profiles ALTER COLUMN ssn DROP MASK;

```
<br></br>
### 6. Critical Operational Caveats

#### Performance Optimization Guidelines

Unity Catalog guarantees data isolation by heavily restricting specific internal physical code optimizations to prevent theoretical "timing attacks" or data leakage via error side-channels. To keep your query engine fast:

* **Minimize Arguments:** Do not pass unnecessary helper columns into your UDF signatures. Databricks cannot prune or optimize columns passed as arguments, forcing extra I/O even if the column is never evaluated by the logic.
* **Keep Logic Inside SQL:** Always prefer pure SQL expressions over Python UDFs. SQL structures undergo advanced query compilation optimizations.
* **Avoid Subqueries:** Rely on simple conditional expressions (`CASE WHEN`) inside your functions rather than looking up permissions against massive side-tables or running deep subqueries.
* **Prevent Runtime Errors:** Avoid operators that throw compilation abort errors (like zero-division). Use safe, fault-tolerant alternatives such as `try_divide`. If an expression can throw a terminal error, the Spark optimizer will refuse to push filters past it in the query plan.

#### Data Type Safety & ANSI Mode

When columns are passed into a UDF, implicit type casting occurs if the signatures do not match perfectly.

* **The Risk:** If ANSI compliance mode is disabled (`spark.sql.ansi.enabled = false`), any casting failure silently generates a `NULL` value. If your row filter returns `NULL`, it can cause severe structural logic gaps where rows are exposed or hidden unexpectedly.
* **The Rule:** Always force strict compliance by enabling `spark.sql.ansi.enabled = true` in your cluster parameters. This ensures type mismatches immediately throw an explicit runtime error rather than masking a silent security failure.

#### Feature Limitations

* **Version Floor:** Requires Databricks Runtime 12.2 LTS or greater. Legacy runtimes will securely fail-closed, blocking access entirely.
* **Incompatible Engine Features:** You cannot combine row/column rules alongside Delta Lake Time Travel operations, Table Clones (deep or shallow), AI Search indexes, or on generated columns.
* **Compute Restrictions:** Shared/Dedicated compute structures require a minimum of Databricks Runtime 15.4 LTS with Serverless configurations for read traffic, and Runtime 16.3 or greater for executing modifications (`INSERT`, `UPDATE`, `DELETE`).
