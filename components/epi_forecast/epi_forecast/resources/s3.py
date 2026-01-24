"""
S3/MinIO Resource for the Epidemiology Forecast Component.

This resource provides S3-compatible storage operations using MinIO client.
"""

import io
from typing import Iterator, Optional

import minio.datatypes
from dagster import get_dagster_logger, ConfigurableResource
from minio import Minio
from pydantic import Field


def PythonMinioAddress(url: str, port: Optional[str] = None) -> str:
    """
    Format URL for MinIO/S3 client.

    Args:
        url: The S3/MinIO server URL
        port: Optional port number

    Returns:
        Formatted URL string for MinIO client
    """
    if url.endswith(".amazonaws.com"):
        python_minio_url = "s3.amazonaws.com"
    else:
        python_minio_url = url
    if port is not None:
        python_minio_url = f"{python_minio_url}:{port}"
    return python_minio_url


class S3Resource(ConfigurableResource):
    """
    S3/MinIO resource for data storage operations.

    This resource wraps the MinIO client to provide S3-compatible storage
    operations for the epidemiology forecast component.
    """

    S3_BUCKET: str = Field(description="Default S3 bucket name")
    S3_ADDRESS: str = Field(description="S3/MinIO server address")
    S3_PORT: str = Field(description="S3/MinIO server port")
    S3_USE_SSL: bool = Field(default=True, description="Whether to use SSL for connections")
    S3_ACCESS_KEY: str = Field(description="S3 access key")
    S3_SECRET_KEY: str = Field(description="S3 secret key")

    def MinioOptions(self) -> dict:
        """Get MinIO connection options."""
        return {
            "secure": self.S3_USE_SSL,
            "access_key": self.S3_ACCESS_KEY,
            "secret_key": self.S3_SECRET_KEY,
        }

    def getClient(self) -> Minio:
        """Get a MinIO client instance."""
        return Minio(
            PythonMinioAddress(self.S3_ADDRESS, self.S3_PORT),
            self.S3_ACCESS_KEY,
            self.S3_SECRET_KEY,
        )

    def baseUrl(self) -> str:
        """Get the base URL for the S3/MinIO server."""
        url = PythonMinioAddress(self.S3_ADDRESS, self.S3_PORT)
        if self.S3_USE_SSL:
            return f"https://{url}"
        else:
            return f"http://{url}"

    def listPath(
        self, path: str = "orgs", recursive: bool = True, bucket: Optional[str] = None
    ) -> Iterator[minio.datatypes.Object]:
        """
        List objects in an S3 path.

        Args:
            path: The S3 path/prefix to list
            recursive: Whether to list recursively
            bucket: Optional bucket name (uses default if None)

        Returns:
            Iterator of MinIO Object metadata
        """
        if bucket is None:
            bucket = self.S3_BUCKET
        result = self.getClient().list_objects(bucket, path)
        return result

    def publicUrl(self, path: str = "test", bucket: Optional[str] = None) -> str:
        """Get the public URL for an S3 object."""
        return f"{self.baseUrl()}/{bucket}/{path}"

    def getFile(self, path: str = "test", bucket: Optional[str] = None) -> bytes:
        """
        Get file contents from S3.

        Args:
            path: The S3 object key/path
            bucket: Optional bucket name (uses default if None)

        Returns:
            File contents as bytes
        """
        if bucket is None:
            bucket = self.S3_BUCKET
        try:
            result = self.getClient().get_object(bucket, path)
            get_dagster_logger().info(f"file {result.status}")
            return result.data
        except Exception as ex:
            get_dagster_logger().info(
                f"file {path} not found in {bucket} at {self.S3_ADDRESS} {ex}"
            )
            raise Exception(
                f"file {path} not found in {bucket} at {self.S3_ADDRESS} {ex}"
            )

    def downloadFile(
        self, path: Optional[str] = None, bucket: str = "test", filename: Optional[str] = None
    ) -> str:
        """
        Download a file from S3 to local filesystem.

        Args:
            path: The S3 object key/path
            bucket: Bucket name
            filename: Local filename to save to

        Returns:
            The local filename
        """
        if bucket is None:
            bucket = self.S3_BUCKET
        try:
            self.getClient().fget_object(bucket, path, file_path=filename)
            get_dagster_logger().info(f"file {filename}")
            return filename
        except Exception as ex:
            get_dagster_logger().error(
                f"file {path} not found in {bucket} at {self.S3_ADDRESS} {ex}"
            )
            raise Exception(
                f"file {path} not found in {bucket} at {self.S3_ADDRESS} {ex}"
            )

    def get_stream(self, path: str = "test", bucket: Optional[str] = None):
        """
        Get a file as a stream object from S3/MinIO.

        Args:
            path: The S3 object key/path
            bucket: Optional bucket name (uses default if None)

        Returns:
            A file-like stream object
        """
        if bucket is None:
            bucket = self.S3_BUCKET
        try:
            result = self.getClient().get_object(bucket, path)
            get_dagster_logger().info(
                f"opened stream for file {path} with status {result.status}"
            )
            return result
        except Exception as ex:
            get_dagster_logger().info(
                f"file {path} not found in {bucket} at {self.S3_ADDRESS} {ex}"
            )
            raise Exception(
                f"file {path} not found in {bucket} at {self.S3_ADDRESS} {ex}"
            )

    def putFile_text(
        self,
        data: str,
        metadata: dict = None,
        path: str = "test",
        content_type: str = "text/plain",
        bucket: Optional[str] = None,
    ) -> str:
        """
        Upload text data to S3.

        Args:
            data: Text content to upload
            metadata: Optional S3 metadata
            path: The S3 object key/path
            content_type: MIME type of the content
            bucket: Optional bucket name (uses default if None)

        Returns:
            The object name that was created
        """
        if metadata is None:
            metadata = {}
        if bucket is None:
            bucket = self.S3_BUCKET
        try:
            result = self.getClient().put_object(
                bucket,
                path,
                data=io.BytesIO(data.encode("utf-8")),
                length=len(data),
                content_type=content_type,
                metadata=metadata,
            )
            get_dagster_logger().info(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name,
                    result.etag,
                    result.version_id,
                ),
            )
            get_dagster_logger().info(f"file {result.object_name}")
            return result.object_name
        except Exception as ex:
            get_dagster_logger().info(
                f"file {path} failed to push to {bucket} at {self.S3_ADDRESS} {ex}"
            )
            raise Exception(
                f"file {path} failed to push to {bucket} at {self.S3_ADDRESS} {ex}"
            )

    def putFile(
        self,
        data: bytes,
        metadata: dict = None,
        path: str = "test",
        content_type: str = "application/octet-stream",
        bucket: Optional[str] = None,
    ) -> str:
        """
        Upload binary data to S3.

        Args:
            data: Binary content to upload
            metadata: Optional S3 metadata
            path: The S3 object key/path
            content_type: MIME type of the content
            bucket: Optional bucket name (uses default if None)

        Returns:
            The object name that was created
        """
        if metadata is None:
            metadata = {}
        if bucket is None:
            bucket = self.S3_BUCKET
        try:
            result = self.getClient().put_object(
                bucket,
                path,
                data=io.BytesIO(data),
                length=len(data),
                content_type=content_type,
                metadata=metadata,
            )
            get_dagster_logger().info(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name,
                    result.etag,
                    result.version_id,
                ),
            )
            get_dagster_logger().info(f"file {result.object_name}")
            return result.object_name
        except Exception as ex:
            get_dagster_logger().info(
                f"file {path} failed to push to {bucket} at {self.S3_ADDRESS} {ex}"
            )
            raise Exception(
                f"file {path} failed to push to {bucket} at {self.S3_ADDRESS} {ex}"
            )

    def putStream(
        self,
        stream,
        length: int = -1,
        metadata: dict = None,
        path: str = "test",
        content_type: str = "application/octet-stream",
        bucket: Optional[str] = None,
    ) -> str:
        """
        Upload data from a stream to S3/MinIO.

        Args:
            stream: A file-like object
            length: Length of data in bytes (-1 for unknown)
            metadata: Optional S3 metadata
            path: The S3 object key/path
            content_type: MIME type of the content
            bucket: Optional bucket name (uses default if None)

        Returns:
            The object name that was created
        """
        if metadata is None:
            metadata = {}
        if bucket is None:
            bucket = self.S3_BUCKET
        try:
            result = self.getClient().put_object(
                bucket,
                path,
                data=stream,
                length=length,
                content_type=content_type,
                metadata=metadata,
            )
            get_dagster_logger().info(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name,
                    result.etag,
                    result.version_id,
                ),
            )
            get_dagster_logger().info(f"file {result.object_name}")
            return result.object_name
        except Exception as ex:
            get_dagster_logger().info(
                f"file {path} failed to push to {bucket} at {self.S3_ADDRESS} {ex}"
            )
            raise Exception(
                f"file {path} failed to push to {bucket} at {self.S3_ADDRESS} {ex}"
            )
