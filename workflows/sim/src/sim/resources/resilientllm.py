
import os
import requests
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import BaseModel, Field

LLM_STATUS_DONE = "done"
LLM_STATUS_ERROR = "error"


class LLMRunHandle(BaseModel):
    """Identity and state of an asynchronous ResilientLLM run.

    The webhook returns ``{"status", "data", "_id", "id"}``; ``id`` is preferred and
    ``_id`` is the fallback so either spelling works.
    """

    id: str
    status: str
    data: dict | None = None

    @classmethod
    def from_response(cls, obj: dict) -> "LLMRunHandle":
        run_id = obj.get("id") or obj.get("_id")
        if not run_id:
            raise ValueError(f"ResilientLLM response has no id: {obj}")
        data = obj.get("data")
        return cls(id=str(run_id), status=str(obj.get("status", "")), data=data if isinstance(data, dict) else None)

    @property
    def is_done(self) -> bool:
        return self.status == LLM_STATUS_DONE

    @property
    def is_error(self) -> bool:
        return self.status == LLM_STATUS_ERROR

    @property
    def is_pending(self) -> bool:
        return not (self.is_done or self.is_error)


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

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def submit(self, data: dict) -> LLMRunHandle:
        """Start an asynchronous run on the configured webhook and return its handle."""
        return LLMRunHandle.from_response(self.execute_with_data(self.webhook_uuid, data))

    def get_status(self, run_id: str) -> LLMRunHandle:
        """Poll ``{webhook}/status`` for a run started by :meth:`submit`.

        POSTs ``{"id": run_id}`` as JSON. The response carries ``status`` and, once
        the run is ``done``, the ``data`` element with the LLM output.
        """
        logger = get_dagster_logger()
        endpoint = f"{self.llm_endpoint}{self.webhook_uuid}/status"
        try:
            response = requests.post(endpoint, headers=self._headers(), json={"id": run_id})
            response.raise_for_status()
            obj = response.json()
        except requests.exceptions.InvalidJSONError as e:
            logger.error(f"ResilientLLM status for {run_id}: invalid JSON {e}")
            raise e
        except requests.exceptions.RequestException as e:
            logger.error(f"ResilientLLM status for {run_id}: {e}")
            raise e
        handle = LLMRunHandle.from_response(obj)
        logger.info(f"ResilientLLM run {run_id} status: {handle.status}")
        return handle

    def execute_with_data(self, report_id: str, data: dict) -> dict:
        """
        Sends a POST request to the ResilientLLM API with a JSON body containing disease data.

        Args:
            report_id: The webhook UUID/report identifier
            data: Dictionary containing disease-specific data to send to LLM

        Returns:
            Parsed JSON response from the API

        Raises:
            requests.exceptions.InvalidJSONError: If response is not valid JSON
            requests.exceptions.RequestException: For HTTP errors
        """
        logger = get_dagster_logger()

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        try:
            endpoint = f"{self.llm_endpoint}{report_id}"
            response = requests.post(endpoint, headers=headers, json=data)
            response.raise_for_status()
            obj = response.json()
            logger.info(f"Successfully posted data to {report_id} ResilientLLM API. Response: {obj}")
            return obj
        except requests.exceptions.InvalidJSONError as e:
            logger.error(f"Failed to post to {report_id} ResilientLLM API: Invalid JSON {e}")
            raise e
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to post to {report_id} ResilientLLM API: {e}")
            raise e
