# target-bigquery

A [Singer](https://singer.io) target that writes data to Google BigQuery.

[![Python package](https://github.com/adswerve/target-bigquery/actions/workflows/python-package.yml/badge.svg)](https://github.com/adswerve/target-bigquery/actions/workflows/python-package.yml)

`target-bigquery` works together with any other [Singer Tap] to move data from sources like [Braintree], [Freshdesk]
and [Hubspot] to Google BigQuery.

## Contents

- [Dependencies](#dependencies)
- [How to use it](#how-to-use-it)
- [Note about BigQuery table/columns names](#note-about-bigquery-tablecolumns-names)


## Dependencies

Install requirements, using either of the two methods below.

**Method 1**

```
pip install -r requirements.txt
```

**Method 2**

Alternatively, you can run the following command. It runs *setup.py* and installs target-bigquery into the env like the
user would. **-e** emulates how a user of the package would install requirements.

```
pip install -e .
```

## BigQuery Setup

### Step 1: Enable Google BigQuery API

1. [GCP web console](https://console.cloud.google.com/) -> **API & Services** -> **Library**

<!-- ![GCP web console -> API & Services -> Library](/readme_screenshots/1_API_and_Services_Library.png?raw=true) -->

<img src="readme_screenshots/01_API_and_Services_Library.png" width="650" alt="GCP web console -> API & Services -> Library">

2. Search for **BigQuery API** -> click **Enable**

<!-- ![Search for BigQuery API](/readme_screenshots/2_Search_for_BigQuery_API.png?raw=true) -->

<img src="readme_screenshots/02_Search_for_BigQuery_API.png" width="650" alt="Search for BigQuery API">

<!-- ![Enable BigQuery API](/readme_screenshots/3_Enable_BigQuery_API.png?raw=true) -->

<img src="readme_screenshots/03_Enable_BigQuery_API.png" width="650" alt="Enable BigQuery API">

### Step 2: Authenticate with a service account

It is recommended to use `target-bigquery` with a service account.

Create a service account credential:

1. **API & Services** -> **Credentials** -> **Create Credentials** -> **Service account**

<!-- ![API & Services -> Credentials -> Create Credentials -> Service account](/readme_screenshots/4_Create_Service_Account.png?raw=true) -->

<img src="readme_screenshots/04_Create_Service_Account.png" width="650" alt="PI & Services -> Credentials -> Create Credentials -> Service account">

2. Under **Service account details**, enter **Service account name**. Click **Create**

<!-- ![Enter Service account name](/readme_screenshots/5_Service_Account_Name.png?raw=true) -->

<img src="readme_screenshots/05_Service_Account_Name.png" width="650" alt="Enter Service account name">

3. Under **Grant this service account access to the project**, select **BigQuery Data Editor** and **BigQuery Job User**
   as the minimal set of permissions. Click **Done**

- **BigQuery Data Editor** permission allows the service account to access (and change) the data.
- **BigQuery Job User** permission allows the service account to actually run a load or select job.

<!-- ![Grant this service account access to the project](/readme_screenshots/6_Service_Account_Access.png?raw=true) -->

<img src="readme_screenshots/06_Service_Account_Access.png" width="650" alt="Grant this service account access to the project">

4. On the **API & Services Credentials** screen, select the service account you just created.

<!-- ![Select the service account](/readme_screenshots/7_Select_Service_Account.png?raw=true) -->

<img src="readme_screenshots/07_Select_Service_Account.png" width="650" alt="Select the service account">

5. Click **ADD KEY** -> **Create new key** -> **JSON key**. Download the service account credential JSON file.

<!-- ![ADD KEY -> Create new key](/readme_screenshots/8_Add_Key.png?raw=true) -->

<img src="readme_screenshots/08_Add_Key.png" width="650" alt="ADD KEY -> Create new key">

<!-- ![JSON key](/readme_screenshots/9_JSON_Key.png?raw=true) -->

<img src="readme_screenshots/09_JSON_Key.png" width="650" alt="JSON key">

<!-- ![Download the service account credential JSON file](/readme_screenshots/10_Download_Client_Secrets.png?raw=true) -->

<img src="readme_screenshots/10_Download_Client_Secrets.png" width="650" alt="Download the service account credential JSON file">

6. Name the file **client_secrets.json**. You can place the file where `target-bigquery` will be executed or provide a
   path to the service account json file.

7. Set a **GOOGLE_APPLICATION_CREDENTIALS** environment variable on the machine, where the value is the fully qualified
   path to **client_secrets.json** file:

- [Creating an environment variable on a Windows 10 machine](https://www.architectryan.com/2018/08/31/how-to-change-environment-variables-on-windows-10/)
- [Creating an environment variable on a Mac machine](https://medium.com/@himanshuagarwal1395/setting-up-environment-variables-in-macos-sierra-f5978369b255)

## Configuration

### Target config file (Main Configuration)

Create a file called **target-config.json** in your working directory, following this
sample [target-config.json](/sample_config/config.json) file (or see the example below).

#### Required Parameters
- **`project_id`** (string): Your Google Cloud Platform project ID
- **`dataset_id`** (string): BigQuery dataset name where tables will be created
- **`google_storage_bucket`** (string): GCS bucket name for staging parquet files

#### Optional Parameters

**BigQuery Configuration:**
- **`location`** (string, default: `"US"`): BigQuery dataset location (e.g., `"US"`, `"EU"`, `"asia-southeast1"`)
- **`table_prefix`** (string, default: `""`): Prefix added to all table names
- **`table_suffix`** (string, default: `""`): Suffix added to all table names
- **`force_alphanumeric_table_names`** (boolean, default: `false`): Force table names to be alphanumeric only

**Replication Method (Global):**
- **`replication_method`** (string, default: `"append"`):
  - `"append"`: Add new rows to existing tables (default)
  - `"truncate"`: Delete all existing rows and insert new ones
  - `"incremental"`: Upsert rows using primary keys (⚠️ **Not recommended**)
- **`truncate_on_full_sync`** (boolean, default: `false`): Force truncate when `SYNC_TYPE=full_sync` environment variable is set

**Data Processing:**
- **`validate_records`** (boolean, default: `true`): Validate incoming records against schema
- **`add_metadata_columns`** (boolean, default: `true`): Add Singer metadata columns (`_sdc_batched_at`, `_sdc_deleted_at`, etc.)

**Google Cloud Storage:**
- **`google_storage_bucket`** (string, default: `"target-bigquery-testing"`): GCS bucket name for staging parquet files
- **`gcs_key_prefix`** (string, optional): Path prefix for uploaded files in GCS (e.g., `"staging/data"` uploads to `gs://bucket/staging/data/stream.parquet`)

**Cross-Account Authentication:**
- **`storage_project_id`** (string, optional): GCP project ID for Cloud Storage bucket (defaults to `project_id` if not specified)
- **`bigquery_credentials_path`** (string, optional): Path to service account JSON file for BigQuery authentication (overrides `GOOGLE_APPLICATION_CREDENTIALS`)
- **`storage_credentials_path`** (string, optional): Path to service account JSON file for Cloud Storage authentication (overrides `GOOGLE_APPLICATION_CREDENTIALS`)

**Table Configuration:**
- **`table_config`** (string): Path to table-specific configuration file (alternative to `--tables` CLI flag)

#### Sample target-config.json file:

```json
{
    "project_id": "{your_GCP_project_id}",
    "dataset_id": "{your_dataset_id}",
    "location": "EU",
    "replication_method": "append",
    "table_prefix": "src_",
    "table_suffix": "_v1",
    "validate_records": true,
    "add_metadata_columns": true,
    "force_alphanumeric_table_names": false,
    "merge_state_messages": true,
    "table_config": "target-tables-config.json"
}
```

#### Minimal Configuration:
```json
{
    "project_id": "my-project",
    "dataset_id": "my_dataset",
    "google_storage_bucket": "my-bucket"
}
```

#### Cross-Account Configuration (Production):

When BigQuery and Cloud Storage are in separate GCP projects/accounts:

```json
{
    "project_id": "bigquery-project-123",
    "dataset_id": "my_dataset",
    "location": "US",
    "google_storage_bucket": "my-staging-bucket",
    "gcs_key_prefix": "singer-target/staging",
    "storage_project_id": "storage-project-456",
    "bigquery_credentials_path": "/path/to/bigquery-service-account.json",
    "storage_credentials_path": "/path/to/storage-service-account.json"
}
```

**Notes:**
- If `storage_project_id` is not specified, it defaults to `project_id`
- If credential paths are not specified, the target falls back to the `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- Both BigQuery and Storage service accounts need appropriate permissions:
  - BigQuery service account: `BigQuery Data Editor` and `BigQuery Job User` roles
  - Storage service account: `Storage Object Admin` role on the bucket

⚠️ **WARNING**: We do not recommend using `incremental` replication method (which uses `MERGE` SQL statement). It might result in loss of production data, because historical records get updated. Instead, we recommend using the `append` replication method, which will preserve historical data.

#### CLI Flags

The following command-line flags can override configuration file settings:

- **`-c, --config`** (required): Path to main target configuration file
- **`-t, --tables`**: Path to table configuration file (overrides `table_config` in main config)
- **`-s, --state`**: Path to initial state file

#### Environment Variables

- **`GOOGLE_APPLICATION_CREDENTIALS`**: Path to Google Cloud service account JSON file
- **`SYNC_TYPE`**: When set to `"full_sync"`, triggers truncate if `truncate_on_full_sync: true` in config

### Table Configuration File

Table-specific configurations can be provided via the `--tables` CLI flag or the `table_config` parameter in the main config file.

#### Usage Options

**Option 1: CLI Flag (Recommended)**
```bash
cat data.singer | ./target-bigquery \
  --config sample_config/config.json  \
  -t sample_config/target-tables-config.json > sample_config/state.json
```

**Option 2: Config File Parameter**
Add `"table_config": "target-tables-config.json"` to your main target-config.json file.

#### Supported Table-Level Properties

**BigQuery Table Properties:**
- **`partition_field`** (string|null): Field name for BigQuery time partitioning (DAY partitioning)
- **`cluster_fields`** (array): List of field names for BigQuery clustering (max 4 fields)
- **`force_fields`** (object): Schema field overrides for specific columns

**Replication Method Overrides (Limited):**
- **`truncate`** (boolean): Force truncate mode for this table (overrides global setting)
- **`replication_method`** (string): Only `"truncate"` value is supported as table-level override

⚠️ **IMPORTANT**: Table-level replication method overrides are **limited**. Only `truncate` can be overridden at the table level. Setting `replication_method` to `"append"` or `"incremental"` at the table level has **no effect** - the global replication method setting will be used instead.


**Partitioning background**

A [partitioned table](https://cloud.google.com/bigquery/docs/partitioned-tables) is a special table that is divided into
segments, called partitions, that make it easier to manage and query your data. By dividing a large table into smaller
partitions, you can:

- improve query performance,
- control costs by reducing the number of bytes read by a query.

You can partition BigQuery tables by:

- Ingestion time: Tables are partitioned based on the data's ingestion (load) time or arrival time.
- Date/timestamp/datetime: Tables are partitioned based on a TIMESTAMP, DATE, or DATETIME column.
- Integer range: Tables are partitioned based on an integer column.

**Clustering background**

- When you create a clustered table in BigQuery, the table data is automatically organized based on the contents of one
  or more columns in the table’s schema.
- The columns you specify are used to colocate related data.
- When you cluster a table using multiple columns, the order of columns you specify is important. The order of the
  specified columns determines the sort order of the data.
- Clustering can improve the performance of certain types of queries such as queries that use filter clauses and queries
  that aggregate data.
- You can cluster up to 4 columns in a table


#### Table Configuration Examples

**Basic Partitioning and Clustering:**
```json
{
    "streams": {
        "charges": {
            "partition_field": "updated_at",
            "cluster_fields": ["type", "status", "customer_id", "transaction_id"]
        }
    }
}
```

We can verify in BigQuery web UI that partitioning and clustering worked:

<img src="readme_screenshots/13_Partitioned_and_Clustered_Table.png" width="650" alt="Download the service account credential JSON file">

Load data data into BigQuery, while configuring target tables.



6. Verify in BigQuery web UI that partitioning and clustering worked (in our example below, we only set up
   partitioning):

<img src="readme_screenshots/14_Partitioned_Table.png" width="650" alt="Download the service account credential JSON file">

#### Schema Field Overrides (`force_fields`)

**Problem:**
- Normally, tap catalog file governs schema of data which will be loaded into target-bigquery
- However, sometimes you can get a column of an undesired data type, which is not following your tap-catalog file

**Solution:**
- You can force that column to the desired data type by using `force_fields` flag inside your target-tables-config.json file

**Example:**
- We used this solution to fix `"date_start"` field from `"ads_insights_age_and_gender"` stream from tap-facebook
- In tap catalog file, we said we wanted this column to be a **date**
- However, the tap generates schema where this column is a **string**, despite our tap catalog file
- Therefore, we used `force_fields` flag in target-tables-config.json to override what the tap generates and force the column to be a date

**Supported Field Types:**
- `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`, `TIMESTAMP`, `DATE`, `TIME`, `DATETIME`, `NUMERIC`, `BIGNUMERIC`, `BYTES`, `RECORD`, `GEOGRAPHY`

**Supported Field Modes:**
- `NULLABLE`: Field can contain null values (default)
- `REQUIRED`: Field cannot contain null values
- `REPEATED`: Field can contain multiple values (array)

## Note about BigQuery table/columns names

BigQuery has some [rules](https://cloud.google.com/bigquery/docs/schemas) about naming tables/columns. For that reason this target writer will:

- Replace special characters with `_`
- Add a `_` prefix when the name starts with `_TABLE_`, `_FILE_`, `_PARTITION_`, `_ROW_TIMESTAMP_`, `__ROOT__`
- Add a `_` prefix if the name doesn't start with a letter or `_`
- Truncate the name to 300 characters if the length is greater than 300
