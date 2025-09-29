import singer

from typing import Dict
from concurrent.futures import ThreadPoolExecutor

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import LoadJobConfig, WriteDisposition, SourceFormat

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
        project_id: str,
        dataset_id: str,
        bucket_name: str,
        location: str,
        parquet_files: Dict[str, str],
    ):
        """
        Initialize the BigQuery loader.

        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID where tables will be created
            bucket_name: Google Cloud Storage bucket name for staging files
            location: Geographic location for BigQuery operations
            parquet_files: Dictionary mapping stream names to parquet file paths
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.parquet_files = parquet_files
        self.uploaded_blob_uris: Dict[str, str] = {}
        self.bq_client = bigquery.Client(project=project_id, location=location)
        self.storage_client = storage.Client(project=project_id)

    def load(self):
        """Execute the complete load process: upload to GCS and immediately start BigQuery load jobs."""

        bucket = self.storage_client.bucket(self.bucket_name)

        def upload_and_load(stream_name: str, parquet_file: str):
            """Upload a file to GCS and immediately start a BigQuery load job for it."""
            logger.info(f"Uploading {stream_name} parquet data to Google Cloud Storage: {parquet_file}")

            # Upload to GCS
            blob_name = f"{stream_name}.parquet"
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(parquet_file)

            source_uri = f"gs://{self.bucket_name}/{blob_name}"
            logger.info(f"Successfully uploaded {stream_name} to GCS: {source_uri}")

            # Immediately start BigQuery load job
            self._create_bigquery_load_job(stream_name, source_uri)

            return stream_name, source_uri

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(upload_and_load, stream_name, parquet_file)
                for stream_name, parquet_file in self.parquet_files.items()
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
                raise Exception(f"Failed {len(failed_operations)} upload/load operations: {'; '.join(failed_operations)}")


    def _create_bigquery_load_job(self, stream_name: str, source_uri: str):
        """Create and execute a BigQuery load job for a single stream.

        Args:
            stream_name: Name of the data stream/table
            source_uri: GCS URI of the parquet file to load
        """
        job_config = LoadJobConfig(
            source_format=SourceFormat.PARQUET,
            write_disposition=WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
        )

        table_id = f"{self.project_id}.{self.dataset_id}.{stream_name}"

        try:
            load_job = self.bq_client.load_table_from_uri(
                source_uri, table_id, job_config=job_config
            )
        except Exception as e:
            logger.error(f"Error creating BigQuery load job for {stream_name}: {e}")
            raise

        load_job.result()
        logger.info(
            f"Successfully loaded {stream_name} into BigQuery table: {table_id}"
        )

