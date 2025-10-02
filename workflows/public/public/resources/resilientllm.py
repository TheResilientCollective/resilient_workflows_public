
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
                              , default="http://52.9.168.22/api/agents/client-controlled-history/")
    summary_id: str = Field(
        description=" Report ID fof the summary ",
        default="6893ee37cdbd1d24e5e8b4be",
    )
    update_id: str = Field(
        description="Report ID fof the update.",
        default="688bfa33bb02b97a05ab7a7f",
    )
    def execute(self,report_id ):
        """
        Sends a ping to the ResilientLLM API.
        """
        logger = get_dagster_logger()
        if not self.token:
            logger.error("RESILIENTLLM_API_TOKEN is not set.")
            raise Exception("RESILIENTLLM_API_TOKEN is not set.")

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        body = {"messages": [{"role": "user", "content": "ping"}]}

        try:
            endpoint = f"{self.llm_endpoint}/{report_id}"
            response = requests.post(endpoint, headers=headers, json=body)
            response.raise_for_status()
            logger.info(f"Successfully generated report for {report_id} ResilientLLM API. Response: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to  generated report for {report_id} ResilientLLM API: {e}")
            raise
