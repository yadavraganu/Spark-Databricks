Governed Tags in Databricks are account-level metadata tags with built-in rules and permissions managed through Unity Catalog. Unlike standard, free-form text tags, they prevent typos and inconsistent casing by forcing users to select from a strict dropdown of administrator-approved values.

## Key Features and How They Work

* **Predefined Values:** Users must select from an approved list of values (e.g., picking confidential from a data_classification menu).
* **Access Control:** Features specific permissions to control who can create tags, manage their allowed values, and assign them to data assets.
* **Tag Inheritance:** Tags applied to high-level assets (like a catalog or schema) automatically cascade down to child tables.
* **Visual Anchor:** Governed tags display a blue lock icon in the user interface, separating them from standard tags.
* **Retroactive Rules:** If a new governed tag is created using an existing standard tag's name, all old instances are instantly brought under the new rule.

## Use Cases and System Limits
### What They Are Used For

* **Attribute-Based Access Control (ABAC):** Powering automated security policies, such as masking columns tagged as confidential.
* **Compliance Tracking:** Working with automatic data scanners to flag sensitive records (like credit card numbers).
* **Cost Accounting:** Enforcing precise codes (e.g., cost_center: marketing) for clean financial reporting.
* **Lifecycle Labeling:** Marking data assets as certified or deprecated using system-governed tags.

### Platform Boundaries

* Limits: Accounts are restricted to 1,000 total governed tags, and each tag can hold up to 50 allowed values.
* Unsupported Items: They cannot be applied to compute resources like Clusters, SQL Warehouses, or Jobs.

```sql
-- Create a tag with specific choices
CREATE GOVERNED TAG compliance_level VALUES ('GDPR', 'HIPAA', 'None');
-- Apply the tag to a target asset
ALTER TABLE main.sales.transactions SET TAGS ('compliance_level' = 'GDPR');
```
