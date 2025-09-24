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

### Target config file

Create a file called **target-config.json** in your working directory, following this
sample [target-config.json](/sample_config/target-config-exchange-rates-api.json) file (or see the example below).

- Required parameters are the project name `project_id` and `dataset_id`.
- Optional parameters are `table_suffix`, `validate records`, `add_metadata_columns`, `location` and `table_config`.
- Default data location is "US" (if your location is not the US, you can indicate a different location in your **
  target-config.json** file).
- The data will be written to the dataset specified in your **target-config.json**.
- If you do not have the dataset with this name yet, it will be created.
- The table will be created.
- There's an optional parameter `replication_method` that can either be:
    * `append`: Adding new rows to the table (Default value)
    * `truncate`: Deleting all previous rows and uploading the new ones to the table
    * `incremental`: **Upserting** new rows into the table, using the **primary key** given by the tap connector
      (if it finds an old row with same key, updates it. Otherwise it inserts the new row)
 - WARNING: We do not recommend using `incremental` option (which uses `MERGE` SQL statement). It might result in loss of production data, because historical records get updated. Instead, we recommend using the `append` replication method, which will preserve historical data.

Sample **target-config.json** file:

```
{
    "project_id": "{your_GCP_project_id}",
    "dataset_id": "{your_dataset_id}",
    "table_suffix": "_sample_table_suffix",
    "validate_records": true,
    "add_metadata_columns": true,
    "location": "EU",
    "table_config": "target-tables-config.json"
}
```

### target-tables-config file

We pass **target-tables-config.json** as a command line argument.

```bash
cat data.singer | ./target-bigquery \
  --config sample_config/target-config-exchange-rates-api.json  \
  -t sample_config/target-tables-config-exchange-rates-api.json > sample_config/state.json
```

- If you don't want to pass **target-tables-config.json** file as a CLI argument, you can
  add ```"table_config": "target-tables-config.json"``` to your **target-config.json** file.


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


#### target-tables-config file: Setting up partitioning and clustering

To configure partitioning and clustering in BigQuery destination tables, we create **target-tables-config.json**:

```
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

##### target-tables-config file: force data types and modes

#### Problem:

- Normally, tap catalog file governs schema of data which will be loaded into target-bigquery.
- However, sometimes you can get a column of an undesired data type, which is not following your tap-catalog file.

#### Solution:

- You can force that column to the desired data type by using `force_fields` flag inside your *
  target-tables-config.json* file.

#### Example:

- We used this solution to fix `"date_start"` field from `"ads_insights_age_and_gender"` stream from tap-facebook.
- In tap catalog file, we said we wanted this column to be a **date**.
- However, the tap generates schema where this column is a **string**, despite our tap catalog file.
- Therefore, we used `force_fields` flag in target-tables-config.json to override what the tap generates and force the
  column to be a date.
- Example of *target-tables-config.json* file:

```
{
    "streams": {
      "ads_insights_age_and_gender": {
        "partition_field": "date_start",
        "cluster_fields": ["age", "gender","account_id", "campaign_id"],
        "force_fields": {
          "date_start": {"type": "DATE", "mode":  "NULLABLE"},
          "date_stop": {"type": "DATE", "mode":  "NULLABLE"}
        }
      }
    }
}
```

## Note about BigQuery table/columns names

BigQuery has some [rules](https://cloud.google.com/bigquery/docs/schemas) about naming tables/columns. For that reason this target writer will:

- Replace special characters with `_`
- Add a `_` prefix when the name starts with `_TABLE_`, `_FILE_`, `_PARTITION_`, `_ROW_TIMESTAMP_`, `__ROOT__`
- Add a `_` prefix if the name doesn't start with a letter or `_`
- Truncate the name to 300 characters if the length is greater than 300
