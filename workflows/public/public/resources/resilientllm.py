
import os
import requests
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field


class ResilientLLMResource(ConfigurableResource):
    """
    A Dagster resource for interacting with the ResilientLLM API.
    """

    token: str = Field(
        description="The API token for the ResilientLLM API.",
        # Default to environment variable
        default_factory=lambda: os.environ.get("RESILIENTLLM_API_TOKEN", ""),
    )
    llm_endpoint: str = Field(description='URL of the ResilientLLM API endpoint.'
                              ,  default_factory=lambda: os.environ.get("RESILIENTLLM_WEBHOOK", " https://n8n.resilienthub.org/webhook/")
                              )
    webhook_uuid: str = Field( description=" Report ID fof the summary ",
        default_factory=lambda: os.environ.get("RESILIENTLLM_WEBHOOK_UUID", ""),
    )

    def execute(self,report_id ):
        """
        Sends a ping to the ResilientLLM API.
        """
        logger = get_dagster_logger()
        if not self.token:
            logger.error("RESILIENTLLM_API_TOKEN is not set.")
            raise Exception("RESILIENTLLM_API_TOKEN is not set.")
        if not self.llm_endpoint:
            logger.error("RESILIENTLLM_WEBHOOK is not set.")
            raise Exception("RESILIENTLLM_WEBHOOK is not set.")

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        #body = {"messages": [{"role": "user", "content": "ping"}]}

        try:
            endpoint = f"{self.llm_endpoint}{report_id}"
            #response = requests.post(endpoint, headers=headers, json=body)
            response = requests.get(endpoint, headers=headers,)
            response.raise_for_status()
            obj = response.json()
            logger.info(f"Successfully generated report for {report_id} ResilientLLM API. Response: {obj}")
            return obj
        except requests.exceptions.InvalidJSONError as e:
            logger.error(f"Failed to  generated report for {report_id} ResilientLLM API: Invalid JSON {e}")
            raise e

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to  generated report for {report_id} ResilientLLM API: {e}")
            raise e
