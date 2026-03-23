import pytest
import requests
import time
import sys
import os
from unittest.mock import Mock, patch, MagicMock

# Add the parent directory to sys.path to import the module directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from resources.resilientsims import  ResilientSimsResource


class TestResilientSimsResource:
    """Test suite for ResilientSimsResource"""

    @pytest.fixture
    def resource_config(self):
        """Basic configuration for ResilientSimsResource"""
        return {
            "RESILIENTSIMS_BASE_URL": "https://test.api.com/api/v1",
            "RESILIENTSIMS_USERNAME": "test_user",
            "RESILIENTSIMS_PASSWORD": "test_pass"
        }

    @pytest.fixture
    def resilient_sims_resource(self, resource_config):
        """Create a ResilientSimsResource instance for testing"""
        return ResilientSimsResource(**resource_config)

    def test_resource_initialization(self, resilient_sims_resource):
        """Test that resource initializes with correct configuration"""
        assert resilient_sims_resource.RESILIENTSIMS_BASE_URL == "https://test.api.com/api/v1"
        assert resilient_sims_resource.RESILIENTSIMS_USERNAME == "test_user"
        assert resilient_sims_resource.RESILIENTSIMS_PASSWORD == "test_pass"
        assert resilient_sims_resource._session is None
        assert resilient_sims_resource._auth_token is None

    @patch('requests.Session')
    def test_get_session_creates_new_session(self, mock_session_class, resilient_sims_resource):
        """Test that _get_session creates a new session and authenticates"""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test_token"}
        mock_session.post.return_value = mock_response

        session = resilient_sims_resource._get_session()

        assert session == mock_session
        assert resilient_sims_resource._session == mock_session
        mock_session.post.assert_called_once()

    @patch('requests.Session')
    def test_authenticate_with_access_token(self, mock_session_class, resilient_sims_resource):
        """Test authentication with access_token response"""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test_access_token"}
        mock_session.post.return_value = mock_response
        resilient_sims_resource._session = mock_session

        resilient_sims_resource._authenticate()

        mock_session.post.assert_called_with(
            "https://test.api.com/api/v1/auth/login/",
            json={"username": "test_user", "password": "test_pass"}
        )
        assert resilient_sims_resource._auth_token == "test_access_token"
        mock_session.headers.update.assert_called_with({
            "Authorization": "Bearer test_access_token"
        })

    @patch('requests.Session')
    def test_authenticate_with_token(self, mock_session_class, resilient_sims_resource):
        """Test authentication with token response"""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        mock_response = Mock()
        mock_response.json.return_value = {"token": "test_token"}
        mock_session.post.return_value = mock_response
        resilient_sims_resource._session = mock_session

        resilient_sims_resource._authenticate()

        assert resilient_sims_resource._auth_token == "test_token"
        mock_session.headers.update.assert_called_with({
            "Authorization": "Token test_token"
        })

    @patch('requests.Session')
    def test_authenticate_session_based(self, mock_session_class, resilient_sims_resource):
        """Test authentication with session-based auth (no token in response)"""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        mock_response = Mock()
        mock_response.json.return_value = {"success": True}
        mock_session.post.return_value = mock_response
        resilient_sims_resource._session = mock_session

        with patch('public.resources.resilientsims.get_dagster_logger') as mock_logger:
            mock_logger.return_value = Mock()
            resilient_sims_resource._authenticate()

        assert resilient_sims_resource._auth_token is None

    @patch('requests.Session')
    def test_authenticate_failure(self, mock_session_class, resilient_sims_resource):
        """Test authentication failure handling"""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        mock_session.post.side_effect = requests.RequestException("Auth failed")
        resilient_sims_resource._session = mock_session

        with pytest.raises(requests.RequestException):
            resilient_sims_resource._authenticate()

    def test_list_simulators_success(self, resilient_sims_resource):
        """Test successful listing of simulators"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": 1, "name": "Simulator 1"},
            {"id": 2, "name": "Simulator 2"}
        ]
        mock_session.get.return_value = mock_response

        with patch.object(resilient_sims_resource, '_get_session', return_value=mock_session):
            simulators = resilient_sims_resource.list_simulators()

        assert len(simulators) == 2
        assert simulators[0]["id"] == 1
        mock_session.get.assert_called_with("https://test.api.com/api/v1/simulators/")

    def test_list_simulators_failure(self, resilient_sims_resource):
        """Test list_simulators with API failure"""
        mock_session = Mock()
        mock_session.get.side_effect = requests.RequestException("API Error")

        with patch.object(resilient_sims_resource, '_get_session', return_value=mock_session):
            with pytest.raises(requests.RequestException):
                resilient_sims_resource.list_simulators()

    def test_verify_simulator_exists_true(self, resilient_sims_resource):
        """Test verify_simulator_exists returns True when simulator exists"""
        simulators = [
            {"id": 1, "name": "Simulator 1"},
            {"pk": "sim2", "name": "Simulator 2"}
        ]

        with patch.object(resilient_sims_resource, 'list_simulators', return_value=simulators):
            assert resilient_sims_resource.verify_simulator_exists("1") is True
            assert resilient_sims_resource.verify_simulator_exists("sim2") is True

    def test_verify_simulator_exists_false(self, resilient_sims_resource):
        """Test verify_simulator_exists returns False when simulator doesn't exist"""
        simulators = [
            {"id": 1, "name": "Simulator 1"},
            {"pk": "sim2", "name": "Simulator 2"}
        ]

        with patch.object(resilient_sims_resource, 'list_simulators', return_value=simulators):
            assert resilient_sims_resource.verify_simulator_exists("999") is False

    def test_create_configuration_success(self, resilient_sims_resource):
        """Test successful configuration creation"""
        mock_session = Mock()
        mock_response = Mock()
        config_data = {"param1": "value1", "param2": "value2"}
        mock_response.json.return_value = {"id": "config123", "simulator": "sim1"}
        mock_session.post.return_value = mock_response

        with patch.object(resilient_sims_resource, '_get_session', return_value=mock_session):
            result = resilient_sims_resource.create_configuration("sim1", config_data)

        assert result["id"] == "config123"
        mock_session.post.assert_called_with(
            "https://test.api.com/api/v1/simulators/sim1/configurations/",
            json=config_data
        )

    def test_create_configuration_failure(self, resilient_sims_resource):
        """Test create_configuration with API failure"""
        mock_session = Mock()
        mock_session.post.side_effect = requests.RequestException("Config creation failed")

        with patch.object(resilient_sims_resource, '_get_session', return_value=mock_session):
            with pytest.raises(requests.RequestException):
                resilient_sims_resource.create_configuration("sim1", {})

    def test_run_simulator_success(self, resilient_sims_resource):
        """Test successful simulator run"""
        mock_session = Mock()
        mock_response = Mock()
        run_data = {"config": "config123"}
        mock_response.json.return_value = {"id": "run456", "status": "started"}
        mock_session.post.return_value = mock_response

        with patch.object(resilient_sims_resource, '_get_session', return_value=mock_session):
            result = resilient_sims_resource.run_simulator("sim1", run_data)

        assert result["id"] == "run456"
        mock_session.post.assert_called_with(
            "https://test.api.com/api/v1/simulators/sim1/runs/",
            json=run_data
        )

    def test_run_simulator_no_data(self, resilient_sims_resource):
        """Test simulator run with no run_data provided"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {"id": "run456", "status": "started"}
        mock_session.post.return_value = mock_response

        with patch.object(resilient_sims_resource, '_get_session', return_value=mock_session):
            result = resilient_sims_resource.run_simulator("sim1")

        mock_session.post.assert_called_with(
            "https://test.api.com/api/v1/simulators/sim1/runs/",
            json={}
        )

    def test_get_run_status_success(self, resilient_sims_resource):
        """Test successful run status retrieval"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {"task_status": "RUNNING", "progress": 50}
        mock_session.get.return_value = mock_response

        with patch.object(resilient_sims_resource, '_get_session', return_value=mock_session):
            result = resilient_sims_resource.get_run_status("sim1", "run456")

        assert result["task_status"] == "RUNNING"
        mock_session.get.assert_called_with(
            "https://test.api.com/api/v1/simulators/sim1/runs/run456/status/"
        )

    @patch('time.sleep')
    def test_monitor_run_until_completion_finished(self, mock_sleep, resilient_sims_resource):
        """Test monitoring run until FINISHED status"""
        status_responses = [
            {"task_status": "PENDING"},
            {"task_status": "RUNNING"},
            {"task_status": "FINISHED"}
        ]

        with patch.object(resilient_sims_resource, 'get_run_status', side_effect=status_responses):
            with patch('time.time', side_effect=[0, 10, 20, 30]):
                result = resilient_sims_resource.monitor_run_until_completion("sim1", "run456")

        assert result["task_status"] == "FINISHED"
        assert mock_sleep.call_count == 2

    @patch('time.sleep')
    def test_monitor_run_until_completion_failed(self, mock_sleep, resilient_sims_resource):
        """Test monitoring run until FAILED status"""
        status_responses = [
            {"task_status": "PENDING"},
            {"task_status": "FAILED"}
        ]

        with patch.object(resilient_sims_resource, 'get_run_status', side_effect=status_responses):
            with pytest.raises(RuntimeError, match="failed with status: FAILED"):
                resilient_sims_resource.monitor_run_until_completion("sim1", "run456")

    @patch('time.sleep')
    def test_monitor_run_until_completion_timeout(self, mock_sleep, resilient_sims_resource):
        """Test monitoring run timeout"""
        status_response = {"task_status": "RUNNING"}

        with patch.object(resilient_sims_resource, 'get_run_status', return_value=status_response):
            with patch('time.time', side_effect=[0, 3700]):  # Exceeds 3600s default timeout
                with pytest.raises(TimeoutError, match="exceeded maximum wait time"):
                    resilient_sims_resource.monitor_run_until_completion("sim1", "run456")

    @patch('time.sleep')
    def test_monitor_run_with_slack_notification_success(self, mock_sleep, resilient_sims_resource):
        """Test monitoring run with Slack notification on success"""
        mock_slack = Mock()
        status_response = {"task_status": "FINISHED"}

        with patch.object(resilient_sims_resource, 'get_run_status', return_value=status_response):
            with patch('time.time', side_effect=[0, 30]):
                result = resilient_sims_resource.monitor_run_until_completion(
                    "sim1", "run456", slack_resource=mock_slack
                )

        mock_slack.send_message.assert_called_once()
        message = mock_slack.send_message.call_args[0][0]
        assert "✅ Simulator Run Completed" in message
        assert "sim1" in message
        assert "run456" in message

    @patch('time.sleep')
    def test_monitor_run_with_slack_notification_failure(self, mock_sleep, resilient_sims_resource):
        """Test monitoring run with Slack notification on failure"""
        mock_slack = Mock()
        status_response = {"task_status": "FAILED"}

        with patch.object(resilient_sims_resource, 'get_run_status', return_value=status_response):
            with pytest.raises(RuntimeError):
                resilient_sims_resource.monitor_run_until_completion(
                    "sim1", "run456", slack_resource=mock_slack
                )

        mock_slack.send_message.assert_called_once()
        message = mock_slack.send_message.call_args[0][0]
        assert "❌ Simulator Run Failed" in message

    def test_run_simulator_workflow_complete(self, resilient_sims_resource):
        """Test complete simulator workflow"""
        config_data = {"param1": "value1"}
        run_data = {"run_param": "value"}
        mock_slack = Mock()

        with patch.object(resilient_sims_resource, 'verify_simulator_exists', return_value=True), \
             patch.object(resilient_sims_resource, 'create_configuration',
                         return_value={"id": "config123"}) as mock_create_config, \
             patch.object(resilient_sims_resource, 'run_simulator',
                         return_value={"id": "run456"}) as mock_run, \
             patch.object(resilient_sims_resource, 'monitor_run_until_completion',
                         return_value={"task_status": "FINISHED"}) as mock_monitor:

            result = resilient_sims_resource.run_simulator_workflow(
                "sim1", config_data, run_data, mock_slack
            )

        assert result["simulator_pk"] == "sim1"
        assert result["config_info"]["id"] == "config123"
        assert result["run_info"]["id"] == "run456"
        assert result["final_status"]["task_status"] == "FINISHED"

        mock_create_config.assert_called_once_with("sim1", config_data)
        mock_run.assert_called_once_with("sim1", run_data)
        mock_monitor.assert_called_once_with("sim1", "run456", slack_resource=mock_slack)

    def test_run_simulator_workflow_no_monitoring(self, resilient_sims_resource):
        """Test simulator workflow without monitoring"""
        with patch.object(resilient_sims_resource, 'verify_simulator_exists', return_value=True), \
             patch.object(resilient_sims_resource, 'run_simulator',
                         return_value={"id": "run456"}) as mock_run, \
             patch.object(resilient_sims_resource, 'monitor_run_until_completion') as mock_monitor:

            result = resilient_sims_resource.run_simulator_workflow(
                "sim1", monitor=False
            )

        assert result["final_status"] is None
        mock_run.assert_called_once()
        mock_monitor.assert_not_called()

    def test_run_simulator_workflow_simulator_not_exists(self, resilient_sims_resource):
        """Test workflow fails when simulator doesn't exist"""
        with patch.object(resilient_sims_resource, 'verify_simulator_exists', return_value=False):
            with pytest.raises(ValueError, match="Simulator sim1 does not exist"):
                resilient_sims_resource.run_simulator_workflow("sim1")

    def test_run_simulator_workflow_no_config(self, resilient_sims_resource):
        """Test workflow without configuration creation"""
        with patch.object(resilient_sims_resource, 'verify_simulator_exists', return_value=True), \
             patch.object(resilient_sims_resource, 'create_configuration') as mock_create_config, \
             patch.object(resilient_sims_resource, 'run_simulator',
                         return_value={"id": "run456"}), \
             patch.object(resilient_sims_resource, 'monitor_run_until_completion',
                         return_value={"task_status": "FINISHED"}):

            result = resilient_sims_resource.run_simulator_workflow("sim1")

        assert result["config_info"] is None
        mock_create_config.assert_not_called()
