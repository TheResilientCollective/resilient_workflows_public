import os
import requests
import time
from typing import Dict, Any, Optional, List
from pydantic import Field
from dagster import get_dagster_logger, ConfigurableResource

SLACK_CHANNEL = os.environ.get("SLACK_SIMS_CHANNEL", "#test")
class ResourceWithResilientSimsConfiguration(ConfigurableResource):
    """Base configuration for ResilientSIMS API"""
    RESILIENTSIMS_SERVER_URL: str = Field(
        default="https://sims.resilientservice.mooo.com",
        description="Base URL for ResilientSIMS API"
    )
    RESILIENTSIMS_API_PATH: str = Field(
        default="/api/v1",
        description="Base URL for ResilientSIMS API"
    )
    RESILIENTSIMS_USERNAME: str = Field(
        description="Username for ResilientSIMS API authentication"
    )
    RESILIENTSIMS_PASSWORD: str = Field(
        description="Password for ResilientSIMS API authentication"
    )
    RESILIENTSIMS_SIMULATOR_ID: int = Field(
        description="identifier of the simulator to run. List simulators api/v1/simulators/"
    )
    RESILIENTSIMS_BUCKET: str = Field(
        description="S3 bucket for output"
    )
class ResilientSimsResource(ResourceWithResilientSimsConfiguration):
    """Resource for interacting with ResilientSIMS API"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._session = None
        self._auth_token = None


    def _get_session(self) -> requests.Session:
        """Get or create HTTP session with authentication"""
        if self._session is None:
            self._session = requests.Session()
            self._authenticate()
        return self._session

    def _authenticate(self) -> None:
        """Authenticate with the ResilientSIMS API"""
        logger = get_dagster_logger()
        if not self.RESILIENTSIMS_USERNAME or not self.RESILIENTSIMS_PASSWORD:
            print("Error: RESILIENTSIMS_USERNAME and RESILIENTSIMS_PASSWORD are required")
            return False
        try:
            # Attempt login/authorization
            auth_url = f"{self.RESILIENTSIMS_SERVER_URL}/admin/login/"
            session = self._session
            # First, get CSRF token from login page
            login_page = session.get(auth_url)
            login_page.raise_for_status()
            # Extract CSRF token from cookies
            csrf_token = session.cookies.get("csrftoken")
            if not csrf_token:
                print("Error: Could not get CSRF token")
                return False
            # Extract token from response (adjust based on actual API response format)
            login_data = {
                "username": self.RESILIENTSIMS_USERNAME,
                "password": self.RESILIENTSIMS_PASSWORD,
                "csrfmiddlewaretoken": csrf_token,
                "next": "/admin/"
            }
            # Set headers for the login request
            headers = {
                "Referer": auth_url,
                "X-CSRFToken": csrf_token,
            }
            response = session.post(auth_url, data=login_data,headers=headers,
                               allow_redirects=False)
            response.raise_for_status()

            if response.status_code in [302, 200] and "sessionid" in self._session.cookies:
                print(f"Successfully logged in as {self.RESILIENTSIMS_USERNAME}")
                # Update session headers for API requests
                session.headers.update({
                    "X-CSRFToken": str(session.cookies.get("csrftoken", "")),
                    "Referer":  f"{self.RESILIENTSIMS_SERVER_URL}{self.RESILIENTSIMS_API_PATH}"
                })
                return True

            elif "access_token" in response:
                self._auth_token = response["access_token"]
                session.headers.update({
                    "Authorization": f"Bearer {self._auth_token}"
                })
            elif "token" in response:
                self._auth_token = response["token"]
                session.headers.update({
                    "Authorization": f"Token {self._auth_token}"
                })
            else:
                # If no explicit token, assume session-based auth worked
                logger.info("Authentication successful (session-based)")

            logger.info("Successfully authenticated with ResilientSIMS API")

        except requests.RequestException as e:
            logger.error(f"Failed to authenticate with ResilientSIMS API: {e}")
            raise

    def list_simulators(self) -> List[Dict[str, Any]]:
        """
        List all available simulators
        GET /api/v1/simulators/
        """
        RESILIENTSIMS_BASE_URL = f"{self.RESILIENTSIMS_SERVER_URL}{self.RESILIENTSIMS_API_PATH}"
        logger = get_dagster_logger()
        session = self._get_session()

        try:
            url = f"{RESILIENTSIMS_BASE_URL}/simulators/"
            response = session.get(url)
            response.raise_for_status()

            simulators = response.json()
            logger.info(f"Retrieved {len(simulators)} simulators")
            return simulators

        except requests.RequestException as e:
            logger.error(f"Failed to list simulators: {e}")
            raise

    def verify_simulator_exists(self, simulator_id: str) -> bool:
        """
        Verify that a simulator with given ID exists
        """
        RESILIENTSIMS_BASE_URL = f"{self.RESILIENTSIMS_SERVER_URL}{self.RESILIENTSIMS_API_PATH}"
        logger = get_dagster_logger()
        simulators = self.list_simulators()

        # Check if simulator_id exists in the list
        for sim in simulators:
            if str(sim.get('id')) == str(simulator_id) or sim.get('pk') == simulator_id:
                logger.info(f"Simulator {simulator_id} found")
                return True

        logger.warning(f"Simulator {simulator_id} not found")
        return False

    def create_configuration(self, simulator_pk: str, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a configuration for a simulator
        POST /api/v1/simulators/{simulator_pk}/configurations/
        """
        RESILIENTSIMS_BASE_URL = f"{self.RESILIENTSIMS_SERVER_URL}{self.RESILIENTSIMS_API_PATH}"
        logger = get_dagster_logger()
        session = self._get_session()

        try:
            url = f"{RESILIENTSIMS_BASE_URL}/simulators/{simulator_pk}/configurations/"
            response = session.post(url, json=config_data)
            response.raise_for_status()

            configuration = response.json()
            logger.info(f"Created configuration {configuration.get('id')} for simulator {simulator_pk}")
            return configuration

        except requests.RequestException as e:
            logger.error(f"Failed to create configuration for simulator {simulator_pk}: {e}")
            raise

    def run_simulator(self, simulator_pk: str, run_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run a simulator
        POST /api/v1/simulators/{simulator_pk}/runs/
        """
        RESILIENTSIMS_BASE_URL = f"{self.RESILIENTSIMS_SERVER_URL}{self.RESILIENTSIMS_API_PATH}"
        logger = get_dagster_logger()
        session = self._get_session()

        try:
            url = f"{RESILIENTSIMS_BASE_URL}/simulators/{simulator_pk}/runs/"

            if run_data is None:
                run_data = {}

            response = session.post(url, json=run_data)
            response.raise_for_status()

            run_info = response.json()
            run_id = run_info.get('id')
            logger.info(f"Started simulator {simulator_pk} run with ID {run_id}")
            return run_info

        except requests.RequestException as e:
            logger.error(f"Failed to run simulator {simulator_pk}: {e}")
            raise

    def get_run_status(self, simulator_pk: str, run_id: str) -> Dict[str, Any]:
        """
        Get the status of a simulator run
        GET /api/v1/simulators/{simulator_pk}/runs/{id}/status/
        """
        RESILIENTSIMS_BASE_URL = f"{self.RESILIENTSIMS_SERVER_URL}{self.RESILIENTSIMS_API_PATH}"
        logger = get_dagster_logger()
        session = self._get_session()

        try:
            url = f"{RESILIENTSIMS_BASE_URL}/simulators/{simulator_pk}/runs/{run_id}/status/"
            response = session.get(url)
            response.raise_for_status()

            status_info = response.json()
            task_status = status_info.get('task_status')
            logger.debug(f"Simulator {simulator_pk} run {run_id} status: {task_status}")
            return status_info

        except requests.RequestException as e:
            logger.error(f"Failed to get status for simulator {simulator_pk} run {run_id}: {e}")
            raise

    def monitor_run_until_completion(
        self,
        simulator_pk: str,
        run_id: str,
        check_interval: int = 30,
        max_wait_time: int = 3600,
        slack_resource=None
    ) -> Dict[str, Any]:
        """
        Monitor a simulator run until completion and optionally send Slack notification

        Args:
            simulator_pk: Simulator primary key
            run_id: Run ID to monitor
            check_interval: Seconds between status checks (default: 30)
            max_wait_time: Maximum time to wait in seconds (default: 3600 = 1 hour)
            slack_resource: Optional Slack resource for notifications

        Returns:
            Final status information
        """

        logger = get_dagster_logger()
        start_time = time.time()

        logger.info(f"Monitoring simulator {simulator_pk} run {run_id} until completion")

        while True:
            try:
                status_info = self.get_run_status(simulator_pk, run_id)
                task_status = status_info.get('task_status')

                if task_status == 'SUCCESS':
                    logger.info(f"Simulator {simulator_pk} run {run_id} completed successfully")

                    # Send Slack notification if resource provided
                    if slack_resource:
                        try:
                            message = f"""
✅ Simulator Run Completed
Simulator: {simulator_pk}
Run ID: {run_id}
Status: {task_status}
Duration: {time.time() - start_time:.0f} seconds
                            """
                            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                        text=message)
                        except Exception as e:
                            logger.warning(f"Failed to send Slack notification: {e}")

                    return status_info

                elif task_status in ['FAILED', 'REVOKED', 'RETRY', 'FAILURE']:
                    error_msg = f"Simulator {simulator_pk} run {run_id} failed with status: {task_status}"
                    logger.error(error_msg)

                    # Send Slack notification if resource provided
                    if slack_resource:
                        try:
                            message = f"""
❌ Simulator Run Failed
Simulator: {simulator_pk}
Run ID: {run_id}
Status: {task_status}
Duration: {time.time() - start_time:.0f} seconds
                            """
                            slack_resource.get_client.chat_postMessage(channel=SLACK_CHANNEL,
                                                        text=message)
                        except Exception as e:
                            logger.warning(f"Failed to send Slack notification: {e}")

                    raise RuntimeError(error_msg)

                elif task_status in ['PENDING', 'STARTED', 'RUNNING', 'PROGRESS']:
                    # Check if we've exceeded maximum wait time
                    if time.time() - start_time > max_wait_time:
                        error_msg = f"Simulator {simulator_pk} run {run_id} exceeded maximum wait time of {max_wait_time} seconds"
                        logger.error(error_msg)
                        raise TimeoutError(error_msg)

                    logger.info(f"Simulator {simulator_pk} run {run_id} still running (status: {task_status})")
                    time.sleep(check_interval)

                else:
                    logger.warning(f"Unknown status for simulator {simulator_pk} run {run_id}: {task_status}")
                    time.sleep(check_interval)

            except (requests.RequestException, RuntimeError, TimeoutError):
                raise
            except Exception as e:
                logger.error(f"Unexpected error monitoring run: {e}")
                time.sleep(check_interval)

    def run_simulator_workflow(
        self,
        simulator_pk: str,
        config_data: Optional[Dict[str, Any]] = None,
        run_data: Optional[Dict[str, Any]] = None,
        slack_resource=None,
        monitor: bool = True,
    ) -> Dict[str, Any]:
        """
        Complete workflow: verify simulator, optionally create config, run, and monitor

        Args:
            simulator_pk: Simulator primary key
            config_data: Optional configuration data to create before running
            run_data: Optional data for the run
            slack_resource: Optional Slack resource for notifications
            monitor: Whether to monitor until completion (default: True)

        Returns:
            Dictionary with run information and final status
        """
        logger = get_dagster_logger()

        # Step 1: Verify simulator exists
        if not self.verify_simulator_exists(simulator_pk):
            raise ValueError(f"Simulator {simulator_pk} does not exist")

        # Step 2: Create configuration if provided
        config_info = None
        if config_data:
            config_info = self.create_configuration(simulator_pk, config_data)
            logger.info(f"Created configuration: {config_info.get('id')}")

        # Step 3: Run simulator
        message=f"""
        🆕 Simulator Run Starting
        Simulator: {simulator_pk}
        """
        slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                     text=message)
        run_info = self.run_simulator(simulator_pk, run_data)
        run_id = run_info.get('id')

        # Step 4: Monitor if requested
        final_status = None
        if monitor:
            final_status = self.monitor_run_until_completion(
                simulator_pk, run_id, slack_resource=slack_resource
            )

        return {
            "simulator_pk": simulator_pk,
            "config_info": config_info,
            "run_info": run_info,
            "final_status": final_status
        }
