import os
import requests
import time
from typing import Dict, Any, Optional, List
from pydantic import Field
from dagster import get_dagster_logger, ConfigurableResource


class ResourceWithResilientSimsConfiguration(ConfigurableResource):
    """Base configuration for ResilientSIMS API"""
    
    RESILIENTSIMS_BASE_URL: str = Field(
        default="https://sims.resilientservice.mooo.com/api/v1",
        description="Base URL for ResilientSIMS API"
    )
    RESILIENTSIMS_USERNAME: str = Field(
        description="Username for ResilientSIMS API authentication"
    )
    RESILIENTSIMS_PASSWORD: str = Field(
        description="Password for ResilientSIMS API authentication"
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
        
        try:
            # Attempt login/authorization
            auth_url = f"{self.RESILIENTSIMS_BASE_URL}/auth/login/"
            auth_data = {
                "username": self.RESILIENTSIMS_USERNAME,
                "password": self.RESILIENTSIMS_PASSWORD
            }
            
            response = self._session.post(auth_url, json=auth_data)
            response.raise_for_status()
            
            # Extract token from response (adjust based on actual API response format)
            auth_response = response.json()
            if "access_token" in auth_response:
                self._auth_token = auth_response["access_token"]
                self._session.headers.update({
                    "Authorization": f"Bearer {self._auth_token}"
                })
            elif "token" in auth_response:
                self._auth_token = auth_response["token"]
                self._session.headers.update({
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
        logger = get_dagster_logger()
        session = self._get_session()
        
        try:
            url = f"{self.RESILIENTSIMS_BASE_URL}/simulators/"
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
        logger = get_dagster_logger()
        session = self._get_session()
        
        try:
            url = f"{self.RESILIENTSIMS_BASE_URL}/simulators/{simulator_pk}/configurations/"
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
        logger = get_dagster_logger()
        session = self._get_session()
        
        try:
            url = f"{self.RESILIENTSIMS_BASE_URL}/simulators/{simulator_pk}/runs/"
            
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
        logger = get_dagster_logger()
        session = self._get_session()
        
        try:
            url = f"{self.RESILIENTSIMS_BASE_URL}/simulators/{simulator_pk}/runs/{run_id}/status/"
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
                
                if task_status == 'FINISHED':
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
                            slack_resource.send_message(message)
                        except Exception as e:
                            logger.warning(f"Failed to send Slack notification: {e}")
                    
                    return status_info
                    
                elif task_status in ['FAILED', 'REVOKED', 'RETRY']:
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
                            slack_resource.send_message(message)
                        except Exception as e:
                            logger.warning(f"Failed to send Slack notification: {e}")
                    
                    raise RuntimeError(error_msg)
                    
                elif task_status in ['PENDING', 'STARTED', 'RUNNING']:
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
        monitor: bool = True
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