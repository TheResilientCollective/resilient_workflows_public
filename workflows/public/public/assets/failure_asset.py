from dagster import asset, Output
import os
# this asset is to test the failure sensor
@asset(key_prefix="testing",)
def my_failure_asset():
    """This asset will fail if the environment variable FAIL_ASSET is set to 'true'."""
    if os.environ.get("FAIL_ASSET", "true") == "true":
        raise Exception("Simulated asset failure for testing.")
    # Normal asset logic here...
    return Output("Asset completed successfully.")
