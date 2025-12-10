import singer
import os
import uuid

from typing import Dict
from concurrent.futures import ThreadPoolExecutor

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import LoadJobConfig, WriteDisposition, SourceFormat, QueryJobConfig
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

from target_bigquery.config import (
    TablesConfig,
    TargetConfig,
    TableConfig,
    ReplicationMethod,
)
from target_bigquery.process import ProcessResult
from target_bigquery.bigquery_schema import create_valid_bigquery_name

logger = singer.get_logger()


class BigQueryLoader:
    """
    Handles uploading parquet files to Google Cloud Storage and loading them into BigQuery.

    This class manages the two-stage process of:
    1. Uploading parquet files to Google Cloud Storage
    2. Loading the files from GCS into BigQuery tables using parallel load jobs
    """

    def __init__(
        self,
        target_config: TargetConfig,
        tables_config: TablesConfig,
        process_result: ProcessResult,
    ):
        """
        Initialize the BigQuery loader.

        Args:
            target_config: Target configuration settings
            tables_config: Table-specific configuration settings
            process_result: Result object from the SingerProcessor.process() method
        """
        self.target_config = target_config
        self.tables_config = tables_config
        self.process_result = process_result
        self.uploaded_blob_uris: Dict[str, str] = {}

        # Load BigQuery credentials
        bq_credentials = None
        if target_config.bigquery_credentials_path:
            bq_credentials = service_account.Credentials.from_service_account_file(
                target_config.bigquery_credentials_path
            )
            logger.info(
                f"Using BigQuery credentials from: {target_config.bigquery_credentials_path}"
            )

        # Load Storage credentials
        storage_credentials = None
        if target_config.storage_credentials_path:
            storage_credentials = service_account.Credentials.from_service_account_file(
                target_config.storage_credentials_path
            )
            logger.info(
                f"Using Storage credentials from: {target_config.storage_credentials_path}"
            )

        # Initialize BigQuery client
        self.bq_client = bigquery.Client(
            project=self.target_config.project_id,
            location=self.target_config.location,
            credentials=bq_credentials,
        )

        # Initialize Storage client (may use different project)
        storage_project = (
            target_config.storage_project_id or target_config.project_id
        )
        self.storage_client = storage.Client(
            project=storage_project,
            credentials=storage_credentials,
        )

    def load(self):
        """Execute the complete load process: upload to GCS and immediately start BigQuery load jobs."""

        bucket = self.storage_client.bucket(self.target_config.google_storage_bucket)

        def upload_and_load(stream_name: str, parquet_file: str):
            """Upload a file to GCS and immediately start a BigQuery load job for it."""
            logger.info(
                f"Uploading {stream_name} parquet data to Google Cloud Storage: {parquet_file}"
            )

            # Upload to GCS with optional key prefix
            blob_name = f"{os.path.basename(self.process_result.parquet_files[stream_name])}"
            if self.target_config.gcs_key_prefix:
                # Ensure prefix doesn't start with / and ends properly
                prefix = self.target_config.gcs_key_prefix.strip("/")
                blob_name = f"{prefix}/{blob_name}"

            blob = bucket.blob(blob_name)
            blob.upload_from_filename(parquet_file)

            source_uri = f"gs://{self.target_config.google_storage_bucket}/{blob_name}"
            logger.info(f"Successfully uploaded {stream_name} to GCS: {source_uri}")

            temp_table_name = None
            try:
                self._create_missing_columns(stream_name)
                temp_table_name = self._create_bigquery_load_job(stream_name, source_uri)
                return stream_name, source_uri
            finally:
                # Clean up temporary table if incremental load was used
                if temp_table_name:
                    try:
                        temp_table_ref = f"{self.target_config.project_id}.{self.target_config.dataset_id}.{temp_table_name}"
                        self.bq_client.delete_table(temp_table_ref, not_found_ok=True)
                        logger.info(f"Successfully deleted temp table: {temp_table_name}")
                    except Exception as e:
                        logger.warning(f"Failed to delete temp table {temp_table_name}: {e}")

                # Clean up GCS file after load completes (success or failure)
                try:
                    blob.delete()
                    logger.info(f"Successfully deleted GCS file: {source_uri}")
                except Exception as e:
                    logger.warning(f"Failed to delete GCS file {source_uri}: {e}")

                # Clean up local parquet file
                try:
                    if os.path.exists(parquet_file):
                        os.remove(parquet_file)
                        logger.info(f"Successfully deleted local parquet file: {parquet_file}")
                except Exception as e:
                    logger.warning(f"Failed to delete local parquet file {parquet_file}: {e}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(upload_and_load, stream_name, parquet_file)
                for stream_name, parquet_file in self.process_result.parquet_files.items()
            ]

            failed_operations = []
            for future in futures:
                try:
                    stream_name, uri = future.result()
                    self.uploaded_blob_uris[stream_name] = uri
                except Exception as e:
                    failed_operations.append(str(e))
                    logger.error(f"Upload and load operation failed: {e}")

            if failed_operations:
                raise Exception(
                    f"Failed {len(failed_operations)} upload/load operations: {'; '.join(failed_operations)}"
                )

    def _create_missing_columns(self, stream):
        table_id = f"{self.target_config.project_id}.{self.target_config.dataset_id}.{self.process_result.table_names[stream]}"

        try:
            table = self.bq_client.get_table(table_id)
        except NotFound:
            return None

        original_schema = table.schema
        new_schema = original_schema[:]

        new_columns = []
        for column in self.process_result.big_query_schemas[stream]:
            if column.name not in [n.name for n in new_schema]:
                new_columns.append(column.name)
                logger.info(f"Column {column.name} missing in table {self.process_result.table_names[stream]}, creating it...")
                new_schema.append(column)
        if new_columns:
            table.schema = new_schema
            try:
                table = self.bq_client.update_table(table, ["schema"])
            except:
                logger.info(f"Error creating column in {self.process_result.table_names[stream]}")

    def _create_bigquery_load_job(self, stream_name: str, source_uri: str) -> str:
        """Create and execute a BigQuery load job for a single stream.

        Args:
            stream_name: Name of the data stream/table
            source_uri: GCS URI of the parquet file to load

        Returns:
            Temporary table name if incremental load, None otherwise
        """
        replication_method = self._get_table_replication_method(stream_name)
        cluster_fields = self._get_table_cluster_fields(stream_name)
        partition_field = self._get_table_partition_field(stream_name)

        # Handle incremental replication with temp table + MERGE
        if replication_method == ReplicationMethod.INCREMENTAL:
            return self._incremental_load_via_temp_table(
                stream_name, source_uri, cluster_fields, partition_field
            )

        # Handle append/truncate replication
        job_config = LoadJobConfig(
            source_format=SourceFormat.PARQUET,
            write_disposition=self._get_write_disposition(replication_method),
            allow_quoted_newlines=True,
            max_bad_records=0,
        )

        if cluster_fields:
            job_config.clustering_fields = cluster_fields

        if partition_field:
            job_config.time_partitioning = bigquery.table.TimePartitioning(
                type_=bigquery.table.TimePartitioningType.DAY, field=partition_field
            )

        table_id = f"{self.target_config.project_id}.{self.target_config.dataset_id}.{self.process_result.table_names[stream_name]}"

        try:
            load_job = self.bq_client.load_table_from_uri(
                source_uri, table_id, job_config=job_config
            )
        except Exception as e:
            logger.error(f"Error creating BigQuery load job for {self.process_result.table_names[stream_name]}: {e}")
            raise

        load_job.result()
        logger.info(
            f"Successfully loaded {stream_name} into BigQuery table: {table_id}"
        )
        return None

    def _incremental_load_via_temp_table(
        self, stream_name: str, source_uri: str, cluster_fields: list[str], partition_field: str
    ) -> str:
        """Load data via temporary table and merge to production.

        Args:
            stream_name: Name of the data stream
            source_uri: GCS URI of the parquet file to load
            cluster_fields: List of fields to cluster on
            partition_field: Field to partition on

        Returns:
            Temporary table name for cleanup
        """
        # Generate temporary table name
        temp_table_name = "t_{}_{}".format(
            self.process_result.table_names[stream_name],
            str(uuid.uuid4()).replace("-", "")
        )
        temp_table_id = f"{self.target_config.project_id}.{self.target_config.dataset_id}.{temp_table_name}"

        # Load to temp table with TRUNCATE
        job_config = LoadJobConfig(
            source_format=SourceFormat.PARQUET,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            allow_quoted_newlines=True,
            max_bad_records=0,
        )

        if cluster_fields:
            job_config.clustering_fields = cluster_fields

        if partition_field:
            job_config.time_partitioning = bigquery.table.TimePartitioning(
                type_=bigquery.table.TimePartitioningType.DAY, field=partition_field
            )

        logger.info(f"Loading {stream_name} to temporary table: {temp_table_name}")

        try:
            load_job = self.bq_client.load_table_from_uri(
                source_uri, temp_table_id, job_config=job_config
            )
            load_job.result()
            logger.info(f"Successfully loaded {stream_name} to temp table: {temp_table_name}")
        except Exception as e:
            logger.error(f"Error loading to temp table {temp_table_name}: {e}")
            raise

        # Merge temp table to production table
        merge_success = self._merge_temp_table_to_production(stream_name, temp_table_name)

        # If merge failed (e.g., production table doesn't exist), copy with TRUNCATE
        if not merge_success:
            logger.info(f"Copying {temp_table_name} to {self.process_result.table_names[stream_name]} with TRUNCATE")
            copy_config = bigquery.CopyJobConfig()
            copy_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

            destination_table_id = f"{self.target_config.project_id}.{self.target_config.dataset_id}.{self.process_result.table_names[stream_name]}"

            copy_job = self.bq_client.copy_table(
                sources=temp_table_id,
                destination=destination_table_id,
                job_config=copy_config
            )
            copy_job.result()
            logger.info(f"Successfully copied {temp_table_name} to {self.process_result.table_names[stream_name]}")

        return temp_table_name

    def _get_table_replication_method(self, stream_name: str) -> ReplicationMethod:
        if (
            self.target_config.replication_method == ReplicationMethod.TRUNCATE
            or self.tables_config.streams.get(
                stream_name, TableConfig()
            ).replication_method
            == "truncate"
            or self.tables_config.streams.get(stream_name, TableConfig()).truncate
        ):
            return ReplicationMethod.TRUNCATE
        elif self.target_config.replication_method == ReplicationMethod.INCREMENTAL:
            if self.process_result.key_properties[stream_name]:
                return ReplicationMethod.INCREMENTAL
            else:
                # If the stream has no key properties, we can't use incremental replication method, so we fallback to truncate.
                # The global replication method fallback is append, so this is a bit confusing, but it preserves the existing behavior.
                return ReplicationMethod.TRUNCATE

        else:
            return ReplicationMethod.APPEND

    def _get_write_disposition(
        self, replication_method: ReplicationMethod
    ) -> WriteDisposition:
        return (
            WriteDisposition.WRITE_APPEND
            if replication_method == ReplicationMethod.APPEND
            else WriteDisposition.WRITE_TRUNCATE
        )

    def _get_table_force_fields(self, stream_name: str) -> dict:
        return self.tables_config.streams.get(stream_name, TableConfig()).force_fields

    def _get_table_partition_field(self, stream_name: str) -> str:
        return self.tables_config.streams.get(
            stream_name, TableConfig()
        ).partition_field

    def _get_table_cluster_fields(self, stream_name: str) -> list[str]:
        return self.tables_config.streams.get(stream_name, TableConfig()).cluster_fields

    def _primary_key_condition(self, stream_name: str) -> str:
        """Generate the ON condition for MERGE statement based on primary keys.

        Args:
            stream_name: Name of the data stream

        Returns:
            SQL condition string like "t.id=s.id AND t.key2=s.key2"

        Raises:
            Exception: If no primary keys specified for incremental replication
        """
        key_properties = [
            create_valid_bigquery_name(k)
            for k in self.process_result.key_properties[stream_name]
        ]
        logger.info(f"Primary keys for {stream_name}: {', '.join(key_properties)}")

        keys = [f"t.{k}=s.{k}" for k in key_properties]
        if len(keys) < 1:
            raise Exception(
                f"No primary keys specified from the tap and Incremental option selected for stream {stream_name}"
            )
        return " and ".join(keys)

    def _merge_temp_table_to_production(
        self, stream_name: str, temp_table_name: str
    ) -> bool:
        """Merge data from temporary table to production table using MERGE statement.

        Args:
            stream_name: Name of the data stream
            temp_table_name: Name of the temporary table containing new data

        Returns:
            True if merge was successful, False if fallback to copy is needed
        """
        table_id = f"{self.target_config.project_id}.{self.target_config.dataset_id}.{self.process_result.table_names[stream_name]}"
        temp_table_id = f"{self.target_config.project_id}.{self.target_config.dataset_id}.{temp_table_name}"

        # Check if production table exists
        try:
            self.bq_client.get_table(table_id)
        except NotFound:
            logger.info(
                f"Table {table_id} not found, will copy temp table with TRUNCATE instead"
            )
            return False

        # Check if we have key properties
        if not self.process_result.key_properties[stream_name]:
            logger.info(
                f"Stream {stream_name} has no key properties, falling back to TRUNCATE"
            )
            return False

        logger.info(f"Merging {temp_table_name} to {self.process_result.table_names[stream_name]} by INCREMENTAL")
        logger.warning(
            f"INCREMENTAL replication method (MERGE SQL statement) is not recommended. "
            f"It might result in loss of production data, because historical records get updated during the sync operation. "
            f"Instead, we recommend using the APPEND replication method, which will preserve historical data."
        )

        # Generate column names for the MERGE statement
        column_names = [x.name for x in self.process_result.big_query_schemas[stream_name]]

        # Build MERGE query
        query = """MERGE `{table}` t
            USING `{temp_table}` s
            ON {primary_key_condition}
            WHEN MATCHED THEN
                UPDATE SET {set_values}
            WHEN NOT MATCHED THEN
                INSERT ({new_cols}) VALUES ({cols})
            """.format(
            table=table_id,
            temp_table=temp_table_id,
            primary_key_condition=self._primary_key_condition(stream_name),
            set_values=", ".join(f"`{c}`=s.`{c}`" for c in column_names),
            new_cols=", ".join(f"`{c}`" for c in column_names),
            cols=", ".join(f"s.`{c}`" for c in column_names),
        )

        # Execute MERGE query
        job_config = QueryJobConfig()
        query_job = self.bq_client.query(query, job_config=job_config)
        query_job.result()

        logger.info(f"LOADED {query_job.num_dml_affected_rows} rows")
        return True
