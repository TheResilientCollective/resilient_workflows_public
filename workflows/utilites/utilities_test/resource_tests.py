import unittest
import os
from dagster import resource, with_resources, Config, build_init_resource_context
from utilities.foursquare import FoursquareResource
@resource(config_schema={"RC_FSQ_REFRESH_TOKEN": os.getenv("RC_FSQ_REFRESH_TOKEN") })
class fsq_resource():
    #RC_FSQ_REFRESH_TOKEN: str = os.getenv("RC_FSQ_REFRESH_TOKEN")
    pass
class FoursquareTestCase(unittest.TestCase):
    def test_list(self):
        with build_init_resource_context(
                resources={"RC_FSQ_REFRESH_TOKEN": os.getenv("RC_FSQ_REFRESH_TOKEN") }
        ) as context:
            FoursquareResource(context)

if __name__ == '__main__':
    unittest.main()
