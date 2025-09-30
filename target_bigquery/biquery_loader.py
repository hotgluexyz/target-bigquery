import singer

from typing import Dict
from concurrent.futures import ThreadPoolExecutor

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import LoadJobConfig, WriteDisposition, SourceFormat
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

from target_bigquery.config import (
    TablesConfig,
    TargetConfig,
    TableConfig,
    ReplicationMethod,
)
from target_bigquery.process import ProcessResult

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
            blob_name = f"{self.process_result.table_names[stream_name]}.parquet"
            if self.target_config.gcs_key_prefix:
                # Ensure prefix doesn't start with / and ends properly
                prefix = self.target_config.gcs_key_prefix.strip("/")
                blob_name = f"{prefix}/{blob_name}"

            blob = bucket.blob(blob_name)
            blob.upload_from_filename(parquet_file)

            source_uri = f"gs://{self.target_config.google_storage_bucket}/{blob_name}"
            logger.info(f"Successfully uploaded {stream_name} to GCS: {source_uri}")

            self._create_missing_columns(stream_name)
            self._create_bigquery_load_job(stream_name, source_uri)

            return stream_name, source_uri

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

    def _create_bigquery_load_job(self, stream_name: str, source_uri: str):
        """Create and execute a BigQuery load job for a single stream.

        Args:
            stream_name: Name of the data stream/table
            source_uri: GCS URI of the parquet file to load
        """
        replication_method = self._get_table_replication_method(stream_name)
        cluster_fields = self._get_table_cluster_fields(stream_name)
        partition_field = self._get_table_partition_field(stream_name)

        if replication_method == ReplicationMethod.INCREMENTAL:
            # TODO: implement incremental replication method
            return

        job_config = LoadJobConfig(
            source_format=SourceFormat.PARQUET,
            write_disposition=self._get_write_disposition(replication_method),
            allow_quoted_newlines=True,
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

    def _get_table_replication_method(self, stream_name: str) -> ReplicationMethod:
        if (
            self.tables_config.streams.get(
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
