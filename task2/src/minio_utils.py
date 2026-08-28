import os
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import load_dotenv

logger = logging.getLogger("minio_utils")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
MINIO_INPUT_BUCKET = os.getenv("MINIO_INPUT_BUCKET", "telemetry-bucket")
MINIO_OUTPUT_BUCKET = os.getenv("MINIO_OUTPUT_BUCKET", "reports-bucket")


class MinIOManager:
    def __init__(self,
                 endpoint_url: str = MINIO_ENDPOINT,
                 access_key: str = MINIO_ACCESS_KEY,
                 secret_key: str = MINIO_SECRET_KEY,
                 region_name: str = "us-east-1"):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.region_name = region_name
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(
                    signature_version="s3v4",
                    retries={"max_attempts": 5, "mode": "standard"},
                    connect_timeout=5,
                    read_timeout=10,
                ),
                region_name=self.region_name,
            )
        return self._client

    def ping(self) -> bool:
        try:
            self.client.list_buckets()
            logger.info("Successfully connected to MinIO server at '%s'", self.endpoint_url)
            return True
        except (EndpointConnectionError, ClientError) as err:
            logger.error("Failed to connect to MinIO Server at '%s': '%s'", self.endpoint_url)
            return False

    def bucket_exists(self, bucket_name: str) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as err:
            error_code = err.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket"):
                return False
            logger.warning("Unexpected ClientError checking bucket '%s': '%s'", bucket_name, err)
            return False

    def ensure_bucket(self, bucket_name: str) -> bool:
        try:
            if not self.bucket_exists(bucket_name):
                logger.info("Bucket '%s' not found. Creating bucket...", bucket_name)
                self.client.create_bucket(Bucket=bucket_name)
                logger.info("Bucket '%s' created successfully.", bucket_name)
            else:
                logger.debug("Bucket '%s' already exists.", bucket_name)
            return True
        except ClientError as err:
            logger.error("Failed to create bucket '%s': '%s'", bucket_name, err)
            return False

    def ensure_pipeline_buckets(self, buckets: Optional[List[str]] = None) -> bool:
        target_buckets = buckets or [MINIO_INPUT_BUCKET, MINIO_OUTPUT_BUCKET]
        all_ok = True
        for b in target_buckets:
            if not self.ensure_bucket(b):
                all_ok=False
        return all_ok

    def upload_file(
            self,
            local_path: str,
            bucket_name: str,
            object_key: Optional[str] = None,
            extra_args: Optional[Dict[str, Any]] = None
    ) -> bool:
        path = Path(local_path)
        if not path.is_file():
            logger.error("Upload aborted: Local path '%s' does not exist or is not a file.", local_path)
            return False

        key = object_key or path.name
        self.ensure_bucket(bucket_name)

        try:
            self.client.upload_file(
                Filename=str(path),
                Bucket=bucket_name,
                Key=key,
                ExtraArgs=extra_args or {}
            )
            file_size_mb = path.stat().st_size / (1024 * 1024)
            logger.info(
                "Uploaded '%s' (%.2f MB) -> 's3://%s/%s'",
                path.name,
                file_size_mb,
                bucket_name,
                key,
            )
            return True
        except ClientError as e:
            logger.error("Failed uploading '%s' to 's3://%s/%s': %s", local_path, bucket_name, key, e)
            return False

    def list_objects(self, bucket_name: str, prefix: str ="") -> List[str]:
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                for item in page.get("Contents", []):
                    keys.append(item["Key"])
            return keys
        except ClientError as e:
            logger.error("Error listing objects in bucket '%s': %s", bucket_name, e)
            return []

    def delete_prefix(self, bucket_name: str, prefix: str) -> int:
        keys = self.list_objects(bucket_name, prefix)
        if not keys:
            return 0

        delete_payload = {"Objects": [{"Key": k} for k in keys]}
        try:
            response = self.client.delete_objects(Bucket=bucket_name, Delete=delete_payload)
            deleted_count = len(response.get("Deleted", []))
            logger.info("Deleted %d objects under prefix '%s' in bucket '%s'", deleted_count, prefix, bucket_name)
            return deleted_count
        except ClientError as e:
            logger.error("Failed to delete objects under prefix '%s': %s", prefix, e)
            return 0

_default_manager = MinIOManager()

def get_s3_client():
    return _default_manager.client

def ensure_bucket_exists(buckets: Optional[List[str]] = None):
    return _default_manager.ensure_pipeline_buckets(buckets)

def upload_file_to_minio(local_file_path: str, bucket_name: str, object_name: Optional[str] = None) -> bool:
    return _default_manager.upload_file(local_file_path, bucket_name, object_name)

