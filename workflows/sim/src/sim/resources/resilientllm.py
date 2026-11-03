import os
import time

import requests
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import BaseModel, Field

LLM_STATUS_DONE = "done"
LLM_FAILED_STATUSES = ("error", "failed", "cancelled", "canceled")


class LLMRunHandle(BaseModel):
    """Identity and state of an asynchronous ResilientLLM run.

    The webhook answers ``{"status", "data", "_id", "id"}``; ``id`` is preferred and
    ``_id`` is the fallback so either spelling works. The same shape comes back from
    the status endpoint, with ``data`` filled in once the run is ``done``.

    Status values seen from the API: "pending", "running",
    "generating human-readable summaries for each disease", "generating translations",
    "done", "cancelled", "error", "failed". Only ``done`` completes a run.
    """

    id: str
    status: str
    data: dict | None = None

    @classmethod
    def from_response(cls, obj) -> "LLMRunHandle":
        # The API sometimes wraps a single object in a one-element list.
        if isinstance(obj, list):
            if len(obj) != 1:
                raise ResilientLLMError(f"Expected a single object in response, got {len(obj)} items: {obj}")
            obj = obj[0]
        if not isinstance(obj, dict):
            raise ResilientLLMError(f"Expected a JSON object in response, got: {obj!r}")
        run_id = obj.get("id") or obj.get("_id")
        if not run_id:
            raise ResilientLLMError(f"Response did not contain a run id: {obj}")
        data = obj.get("data")
        return cls(
            id=str(run_id),
            status=str(obj.get("status", "")),
            data=data if isinstance(data, dict) else None,
        )

    @property
    def is_done(self) -> bool:
        return self.status == LLM_STATUS_DONE

    @property
    def is_error(self) -> bool:
        return self.status.lower() in LLM_FAILED_STATUSES

    @property
    def is_pending(self) -> bool:
        return not (self.is_done or self.is_error)


class ResilientLLMError(Exception):
    """Raised when the ResilientLLM API returns an unusable response or a run fails."""


class ResilientLLMResource(ConfigurableResource):
    """
    A Dagster resource for interacting with the ResilientLLM API.

    The API is asynchronous: a request to ``<webhook>/<report_id>/<version>`` starts
    a run and returns a run id immediately. The run is then polled at
    ``<webhook>/<report_id>/<version>/status`` (POST ``{"id": run_id}``) until its
    ``status`` is ``"done"``, at which point ``data`` holds the report.

    Two ways to use it:

    * asynchronous (San Diego by-disease flow): ``submit()`` returns an
      :class:`LLMRunHandle`; a sensor later calls ``get_status()`` until the handle
      ``is_done``;
    * blocking (mpox): ``execute_with_data()`` submits and polls in-process.
    """

    token: str = Field(
        description="The API token for the ResilientLLM API.",
        default_factory=lambda: os.environ.get("RESILIENTLLM_API_TOKEN", ""),
    )
    llm_endpoint: str = Field(
        description="Base URL of the ResilientLLM webhook endpoint (ends with a slash).",
        default_factory=lambda: os.environ.get(
            "RESILIENTLLM_WEBHOOK", "https://agents.resilienthub.org/webhook/"
        ),
    )
    webhook_uuid: str = Field(
        description="Report ID (webhook UUID) for the summary.",
        default_factory=lambda: os.environ.get("RESILIENTLLM_WEBHOOK_UUID", ""),
    )
    api_version: str = Field(
        description="Version path segment appended after the webhook UUID.",
        default_factory=lambda: os.environ.get("RESILIENTLLM_API_VERSION", "v3"),
    )
    status_path: str = Field(
        description="Path segment appended to the run endpoint to poll status.",
        default="status",
    )
    poll_interval_seconds: int = Field(
        description="Seconds to wait between status polls (blocking calls only).",
        default_factory=lambda: int(os.environ.get("RESILIENTLLM_POLL_INTERVAL", "30")),
    )
    timeout_seconds: int = Field(
        description="Maximum seconds to wait for a run to reach 'done' (blocking calls only).",
        default_factory=lambda: int(os.environ.get("RESILIENTLLM_TIMEOUT", "3600")),
    )
    request_timeout_seconds: int = Field(
        description="Per-HTTP-request timeout in seconds.",
        default=120,
    )

    # ------------------------------------------------------------------ helpers

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _check_config(self, report_id: str) -> None:
        logger = get_dagster_logger()
        if not self.token:
            logger.error("RESILIENTLLM_API_TOKEN is not set.")
            raise ResilientLLMError("RESILIENTLLM_API_TOKEN is not set.")
        if not self.llm_endpoint:
            logger.error("RESILIENTLLM_WEBHOOK is not set.")
            raise ResilientLLMError("RESILIENTLLM_WEBHOOK is not set.")
        if not report_id:
            logger.error("ResilientLLM report id (webhook UUID) is empty.")
            raise ResilientLLMError("ResilientLLM report id (webhook UUID) is empty.")

    def run_endpoint(self, report_id: str) -> str:
        base = self.llm_endpoint.strip().rstrip("/")
        url = f"{base}/{report_id}"
        if self.api_version:
            url = f"{url}/{self.api_version.strip('/')}"
        return url

    def status_endpoint(self, report_id: str) -> str:
        return f"{self.run_endpoint(report_id)}/{self.status_path.strip('/')}"

    # --------------------------------------------------------------- API calls

    def submit(self, report_id: str, data: dict | None = None) -> LLMRunHandle:
        """
        Start a run and return immediately.

        POSTs ``data`` (if given) to the run endpoint, otherwise GETs it.

        Returns:
            The run handle; ``handle.id`` is what the status endpoint is polled with.
        """
        logger = get_dagster_logger()
        self._check_config(report_id)
        endpoint = self.run_endpoint(report_id)
        try:
            if data is None:
                response = requests.get(endpoint, headers=self._headers(), timeout=self.request_timeout_seconds)
            else:
                response = requests.post(endpoint, headers=self._headers(), json=data, timeout=self.request_timeout_seconds)
            response.raise_for_status()
            obj = response.json()
        except requests.exceptions.InvalidJSONError as e:
            logger.error(f"Failed to start ResilientLLM run for {report_id}: Invalid JSON {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to start ResilientLLM run for {report_id}: {e}")
            raise
        handle = LLMRunHandle.from_response(obj)
        logger.info(f"Started ResilientLLM run {handle.id} for {report_id} (status: {handle.status!r})")
        return handle

    def get_status(self, report_id: str, run_id: str) -> LLMRunHandle:
        """
        Fetch the current state of a run started by :meth:`submit`.

        POSTs ``{"id": run_id}`` to the status endpoint. The handle carries ``status``
        and, once the run is ``done``, the ``data`` element with the LLM output.
        """
        logger = get_dagster_logger()
        self._check_config(report_id)
        endpoint = self.status_endpoint(report_id)
        try:
            response = requests.post(endpoint, headers=self._headers(), json={"id": run_id}, timeout=self.request_timeout_seconds)
            response.raise_for_status()
            obj = response.json()
        except requests.exceptions.InvalidJSONError as e:
            logger.error(f"Failed to get status of ResilientLLM run {run_id}: Invalid JSON {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get status of ResilientLLM run {run_id}: {e}")
            raise
        handle = LLMRunHandle.from_response(obj)
        logger.debug(f"ResilientLLM run {run_id} status: {handle.status!r}")
        return handle

    def wait_for_completion(self, report_id: str, run_id: str) -> dict:
        """
        Block, polling the status endpoint until the run is ``done``.

        Returns:
            The ``data`` payload of the completed run.

        Raises:
            ResilientLLMError: on a failed status or when ``timeout_seconds`` elapses.
        """
        logger = get_dagster_logger()
        deadline = time.monotonic() + self.timeout_seconds
        last_status = None
        poll = 0
        while True:
            poll += 1
            handle = self.get_status(report_id, run_id)
            if handle.status != last_status:
                logger.info(f"ResilientLLM run {run_id} status: {handle.status!r}")
                last_status = handle.status

            if handle.is_done:
                logger.info(f"ResilientLLM run {run_id} completed after {poll} poll(s).")
                return handle.data
            if handle.is_error:
                raise ResilientLLMError(f"ResilientLLM run {run_id} failed with status {handle.status!r}")

            if time.monotonic() >= deadline:
                raise ResilientLLMError(
                    f"ResilientLLM run {run_id} did not reach {LLM_STATUS_DONE!r} within "
                    f"{self.timeout_seconds}s (last status: {handle.status!r})"
                )
            time.sleep(self.poll_interval_seconds)

    # ------------------------------------------------------------ blocking API

    def execute(self, report_id: str):
        """Start a run with no request body and block until it finishes; returns ``data``."""
        handle = self.submit(report_id)
        return self.wait_for_completion(report_id, handle.id)

    def execute_with_data(self, report_id: str, data: dict) -> dict:
        """
        Start a run with a JSON body containing disease data and block until it finishes.

        Args:
            report_id: The webhook UUID/report identifier
            data: Dictionary containing disease-specific data to send to LLM

        Returns:
            The ``data`` payload of the completed run (keyed by language, e.g. ``en``/``es``).

        Raises:
            requests.exceptions.RequestException: for HTTP errors
            ResilientLLMError: if the run fails or times out
        """
        handle = self.submit(report_id, data)
        return self.wait_for_completion(report_id, handle.id)
