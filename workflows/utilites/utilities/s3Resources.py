from dagster import asset, get_dagster_logger, define_asset_job, ConfigurableResource, contextmanager
from dagster_aws.s3 import  S3Resource

#from dagster import Field
from pydantic import Field
import minio
import logging

def PythonMinioAddress(url, port=None):
    if (url.endswith(".amazonaws.com")):
        PYTHON_MINIO_URL = "s3.amazonaws.com"
    else:
        PYTHON_MINIO_URL = url

    if port is not None and port != "":
        PYTHON_MINIO_URL = f"{PYTHON_MINIO_URL}:{port}"

    return PYTHON_MINIO_URL
class S3Client():
    def __init__(self, s3endpoint, options={}, default_bucket="resilienttest") :
        """ Initilize with
        Parameters:
            s3endpoint: endpoint. If this is aws, include the region. eg s3.us-west-2.amazon....
            options: creditials, other parameters to pass to client
            default_bucket: 'gleaner'
            """
        self.endpoint = s3endpoint
        self.options = {} if options is None else options  # old code has none...
        self.default_bucket = default_bucket
        logging.info(str(options))
        self.s3client = minio.Minio(s3endpoint, **self.options)  # this will neeed to be fixed with authentication

        ## https://docs.dagster.io/_apidocs/libraries/dagster-aws#s3
        #   fields from dagster_aws.s3.S3Resource
        # region_name
        # endpoint_url
        # use_ssl
        # aws_access_key_id
        # aws_secret_access_key

    def listPath(self, path='orgs', recusrsive=True, start_after=None):
        objects = self.s3client.list_objects(
            bucket_name=self.RC_S3_BUCKET,
            prefix=path,
            recusrsive=recusrsive,
            start_after=start_after
        )
        return objects
    def countPath(self, path='orgs', recusrsive=True):
        objects = self.s3client.list_objects(
            bucket_name=self.RC_S3_BUCKET,
            prefix=path,
            recusrsive=recusrsive
        )
        return len(objects)

    def getFile(self, path='test'):
        try:
            object = self.s3client.fget_object(
                bucket_name=self.RC_S3_BUCKET,
                object_name=path,
            )
            return object
        except Exception as ex:
            get_dagster_logger().info(f"file {path} not found  in {self.RC_S3_BUCKET} at {self.s3.endpoint_url} {ex}")

    def putFile(self, path='test'):
        pass
class S3Resource(ConfigurableResource):
    # this should be s3, since it is the s3 resource. Others at gleaner s3 resources
    RC_S3_BUCKET: str =  Field(
         description="S3_BUCKET", default="resilientprivate")
    RC_S3_ADDRESS: str =  Field(
         description="S3_BUCKET.")
    RC_S3_PORT: str =  Field(
         description="S3_BUCKET.")
    RC_S3_USE_SSL: bool=  Field(
         default=False, description="S3_USE_SSL")
    RC_S3_ACCESS_KEY: str = Field(
        description="S3_ACCESS_KEY")
    RC_S3_SECRET_KEY: str = Field(
        description="S3_SECRET_KEY")

    @contextmanager
    def get_client(self) -> S3Client:
        options={"secure": self.RC_S3_USE_SSL
            , "access_key":  self.RC_S3_ACCESS_KEY
            , "secret_key": self.RC_S3_SECRET_KEY
             }

        endpoint = PythonMinioAddress(self.RC_S3_ADDRESS, self.RC_S3_PORT)
        return   S3Client(endpoint, options=options, default_bucket=self.RC_S3_BUCKET)

    ## https://docs.dagster.io/_apidocs/libraries/dagster-a



