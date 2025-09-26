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
        """Execute the complete load process: upload to GCS then load to BigQuery."""
        self._upload_parquet_files_to_google_cloud_storage()
        self._load_parquet_files_to_bigquery()

    def _upload_parquet_files_to_google_cloud_storage(self):
        """Upload all parquet files to Google Cloud Storage sequentially."""
        bucket = self.storage_client.bucket(self.bucket_name)

        for stream_name, parquet_file in self.parquet_files.items():
            logger.info(
                f"Uploading {stream_name} parquet data to Google Cloud Storage: {parquet_file}"
            )

            blob_name = f"{stream_name}.parquet"

            blob = bucket.blob(blob_name)
            blob.upload_from_filename(parquet_file)

            self.uploaded_blob_uris[stream_name] = (
                f"gs://{self.bucket_name}/{blob_name}"
            )
            logger.info(
                f"Successfully uploaded {stream_name} to GCS: {self.uploaded_blob_uris[stream_name]}"
            )

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

    def _load_parquet_files_to_bigquery(self):
        """Load all uploaded parquet files to BigQuery using parallel load jobs.

        Uses ThreadPoolExecutor to run multiple load jobs concurrently.
        Collects and reports any failures after all jobs complete.
        """

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(self._create_bigquery_load_job, stream_name, source_uri)
                for stream_name, source_uri in self.uploaded_blob_uris.items()
            ]

            failed_jobs = []
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    failed_jobs.append(str(e))
                    logger.error(f"BigQuery load job failed: {e}")

            if failed_jobs:
                raise Exception(
                    f"Failed to load {len(failed_jobs)} tables to BigQuery: {'; '.join(failed_jobs)}"
                )
