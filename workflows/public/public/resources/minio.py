from dagster import asset, get_dagster_logger, define_asset_job, ConfigurableResource
from minio import Minio
import io
#from dagster import Field
from pydantic import Field,ConfigDict


def PythonMinioAddress(url, port=None):
    if (url.endswith(".amazonaws.com")):
        PYTHON_MINIO_URL = "s3.amazonaws.com"
    else:
        PYTHON_MINIO_URL = url
    if port is not None:
        PYTHON_MINIO_URL = f"{PYTHON_MINIO_URL}:{port}"
    return PYTHON_MINIO_URL




class ResourceWithS3Configuration(ConfigurableResource):

    # this should be s3, since it is the s3 resource. Others at gleaner s3 resources
    S3_BUCKET: str =  Field(
         description="S3_BUCKET.")
    S3_ADDRESS: str =  Field(
         description="S3_HOST NAME.")
    S3_PORT: str =  Field(
         description="S3_PORT.")
    S3_USE_SSL: bool=  Field(
         default=False)
    S3_ACCESS_KEY: str = Field(
        description="S3_ACCESS_KEY")
    S3_SECRET_KEY: str = Field(
        description="S3_SECRET_KEY")
    ## https://docs.dagster.io/_apidocs/libraries/dagster-a

class S3Resource(ResourceWithS3Configuration):

    def MinioOptions(self):
        return  {"secure": self.S3_USE_SSL

            , "access_key":  self.S3_ACCESS_KEY
            , "secret_key": self.S3_SECRET_KEY
                         }
    def getClient(self):
        return     Minio(PythonMinioAddress(self.S3_ADDRESS, self.S3_PORT), self.S3_ACCESS_KEY, self.S3_SECRET_KEY)

## https://docs.dagster.io/_apidocs/libraries/dagster-aws#s3
#   fields from dagster_aws.s3.S3Resource
# region_name
# endpoint_url
# use_ssl
# aws_access_key_id
# aws_secret_access_key
    def listPath(self, path='orgs', recusrsive=True):
        result = self.getClient().list_objects(
            Bucket=self.S3_BUCKET,
            Prefix=path,
#            Recusrsive=recusrsive
        )
        return result["Contents"]
    def getFile(self, path='test'):
        try:
            result =   self.getClient().get_object(
                self.S3_BUCKET,
                path,
            )
            get_dagster_logger().info(
                f"file {result.status}" )
            return result.data
        except Exception as ex:
            get_dagster_logger().info(f"file {path} not found  in {self.S3_BUCKET} at {self.S3_ADDRESS} {ex}")
            raise Exception(f"file {path} not found  in {self.S3_BUCKET} at {self.S3_ADDRESS} {ex}")

# note metadata is S3 metadata not JSONLD metadata
    def putFile_text(self, data, metadata={}, path='test'):
        try:
            result =  self.getClient().put_object(
                self.S3_BUCKET, path,
                data=io.BytesIO(data.encode('utf-8')),
                length=len(data),
                content_type="text/plain", metadata=metadata
            )
            get_dagster_logger().info(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name, result.etag, result.version_id,
                ),
            )
            get_dagster_logger().info(
                f"file {result.object_name}" )
            return result.object_name
        except Exception as ex:
            get_dagster_logger().info(f"file {path} failed to push  to {self.S3_BUCKET} at {self.S3_ADDRESS} {ex}")
            raise Exception(f"file {path} failed to push  to {self.S3_BUCKET} at {self.S3_ADDRESS} {ex}")

    def putFile(self, data, metadata={}, path='test', content_type='application/octet-stream'):
        try:
            result =  self.getClient().put_object(
                self.S3_BUCKET, path,
                data=io.BytesIO(data),
                length=len(data),
                content_type=content_type, metadata=metadata
            )
            get_dagster_logger().info(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name, result.etag, result.version_id,
                ),
            )
            get_dagster_logger().info(
                f"file {result.object_name}" )
            return result.object_name
        except Exception as ex:
            get_dagster_logger().info(f"file {path} failed to push  to {self.S3_BUCKET} at {self.S3_ADDRESS} {ex}")
            raise Exception(f"file {path} failed to push  to {self.S3_BUCKET} at {self.S3_ADDRESS} {ex}")
