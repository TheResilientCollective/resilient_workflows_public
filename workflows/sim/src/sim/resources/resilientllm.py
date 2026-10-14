import os
import time

import requests
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field


class ResilientLLMError(Exception):
    """Raised when the ResilientLLM API returns an unusable response or a run fails."""


class ResilientLLMResource(ConfigurableResource):
    """
    A Dagster resource for interacting with the ResilientLLM API.

    The API is asynchronous: a request to the webhook starts a run and returns
    a run id immediately. The run is then polled at ``<webhook>/status`` until
    its ``status`` is ``"done"``, at which point ``data`` holds the report.
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
        description="Seconds to wait between status polls.",
        default_factory=lambda: int(os.environ.get("RESILIENTLLM_POLL_INTERVAL", "30")),
    )
    timeout_seconds: int = Field(
        description="Maximum seconds to wait for a run to reach 'done'.",
        default_factory=lambda: int(os.environ.get("RESILIENTLLM_TIMEOUT", "3600")),
    )
    request_timeout_seconds: int = Field(
        description="Per-HTTP-request timeout in seconds.",
        default=120,
    )
    done_status: str = Field(
        description="Status value that marks a completed run.",
        default="done",
    )
    failed_statuses: list[str] = Field(
        description="Status values that mark a failed run (case-insensitive).",
        default=["error", "failed", "cancelled", "canceled"],
    )

    # ------------------------------------------------------------------ helpers

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _check_config(self) -> None:
        logger = get_dagster_logger()
        if not self.token:
            logger.error("RESILIENTLLM_API_TOKEN is not set.")
            raise ResilientLLMError("RESILIENTLLM_API_TOKEN is not set.")
        if not self.llm_endpoint:
            logger.error("RESILIENTLLM_WEBHOOK is not set.")
            raise ResilientLLMError("RESILIENTLLM_WEBHOOK is not set.")

    def run_endpoint(self, report_id: str) -> str:
        base = self.llm_endpoint.strip().rstrip("/")
        url = f"{base}/{report_id}"
        if self.api_version:
            url = f"{url}/{self.api_version.strip('/')}"
        return url

    def status_endpoint(self, report_id: str) -> str:
        return f"{self.run_endpoint(report_id)}/{self.status_path.strip('/')}"

    @staticmethod
    def _unwrap(obj):
        """The API sometimes wraps a single object in a one-element list."""
        if isinstance(obj, list):
            if len(obj) != 1:
                raise ResilientLLMError(f"Expected a single object in response, got {len(obj)} items: {obj}")
            obj = obj[0]
        if not isinstance(obj, dict):
            raise ResilientLLMError(f"Expected a JSON object in response, got: {obj!r}")
        return obj

    @staticmethod
    def _run_id(obj: dict) -> str:
        run_id = obj.get("id") or obj.get("_id")
        if not run_id:
            raise ResilientLLMError(f"Response did not contain a run id: {obj}")
        return str(run_id)

    # --------------------------------------------------------------- API calls

    def submit(self, report_id: str, data: dict | None = None) -> str:
        """
        Start a run. POSTs ``data`` (if given) to the run endpoint, otherwise GETs it.

        Returns:
            The run id to poll with.
        """
        logger = get_dagster_logger()
        self._check_config()
        endpoint = self.run_endpoint(report_id)
        try:
            if data is None:
                response = requests.get(endpoint, headers=self._headers(), timeout=self.request_timeout_seconds)
            else:
                response = requests.post(endpoint, headers=self._headers(), json=data, timeout=self.request_timeout_seconds)
            response.raise_for_status()
            obj = self._unwrap(response.json())
        except requests.exceptions.InvalidJSONError as e:
            logger.error(f"Failed to start ResilientLLM run for {report_id}: Invalid JSON {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to start ResilientLLM run for {report_id}: {e}")
            raise
        run_id = self._run_id(obj)
        logger.info(f"Started ResilientLLM run {run_id} for {report_id} (status: {obj.get('status')})")
        return run_id

    def get_status(self, report_id: str, run_id: str) -> dict:
        """
        Fetch the current state of a run.

        Returns:
            Dict with at least ``status`` and ``data`` keys.
        """
        logger = get_dagster_logger()
        endpoint = self.status_endpoint(report_id)
        try:
            response = requests.post(endpoint, headers=self._headers(), json={"id": run_id}, timeout=self.request_timeout_seconds)
            response.raise_for_status()
            return self._unwrap(response.json())
        except requests.exceptions.InvalidJSONError as e:
            logger.error(f"Failed to get status of ResilientLLM run {run_id}: Invalid JSON {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get status of ResilientLLM run {run_id}: {e}")
            raise

    def wait_for_completion(self, report_id: str, run_id: str) -> dict:
        """
        Poll the status endpoint until the run's status equals ``done_status``.

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
            state = self.get_status(report_id, run_id)
            status = state.get("status")
            if status != last_status:
                logger.info(f"ResilientLLM run {run_id} status: {status!r}")
                last_status = status
            else:
                logger.debug(f"ResilientLLM run {run_id} status (poll {poll}): {status!r}")

            if status == self.done_status:
                logger.info(f"ResilientLLM run {run_id} completed after {poll} poll(s).")
                return state.get("data")
            if isinstance(status, str) and status.lower() in [s.lower() for s in self.failed_statuses]:
                raise ResilientLLMError(f"ResilientLLM run {run_id} failed with status {status!r}: {state}")

            if time.monotonic() >= deadline:
                raise ResilientLLMError(
                    f"ResilientLLM run {run_id} did not reach {self.done_status!r} within "
                    f"{self.timeout_seconds}s (last status: {status!r})"
                )
            time.sleep(self.poll_interval_seconds)

    # ------------------------------------------------------------ public API

    def execute(self, report_id: str):
        """
        Start a run with no request body and wait for it to finish.

        Returns:
            The ``data`` payload of the completed run.
        """
        run_id = self.submit(report_id)
        return self.wait_for_completion(report_id, run_id)

    def execute_with_data(self, report_id: str, data: dict) -> dict:
        """
        Start a run with a JSON body containing disease data and wait for it to finish.

        Args:
            report_id: The webhook UUID/report identifier
            data: Dictionary containing disease-specific data to send to LLM

        Returns:
            The ``data`` payload of the completed run (keyed by language, e.g. ``en``/``es``).

        Raises:
            requests.exceptions.RequestException: for HTTP errors
            ResilientLLMError: if the run fails or times out
        """
        run_id = self.submit(report_id, data)
        return self.wait_for_completion(report_id, run_id)
