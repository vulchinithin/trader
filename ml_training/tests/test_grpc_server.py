import pytest
from unittest.mock import patch, MagicMock
import grpc

# Add project root to path to allow importing from other services
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ml_training.grpc_server import OrchestratorServicer
from common.generated import orchestrator_pb2

@pytest.fixture
def servicer():
    """Create an instance of our servicer."""
    return OrchestratorServicer()

@patch('ml_training.grpc_server.subprocess.Popen')
def test_trigger_task_train_model_success(mock_popen, servicer):
    """
    Test the successful handling of a 'train_model' trigger.
    """
    # --- Mock Setup ---
    request = orchestrator_pb2.TriggerRequest(
        task_name="train_model",
        parameters={"symbol": "ETHUSDT", "tune": "true"}
    )
    context = MagicMock()

    # --- Test Execution ---
    response = servicer.TriggerTask(request, context)

    # --- Assertions ---
    # Verify that subprocess.Popen was called
    mock_popen.assert_called_once()

    # Verify the command passed to Popen
    command_arg = mock_popen.call_args[0][0]
    assert "python -m ml_training.train" in command_arg
    assert "--symbol ETHUSDT" in command_arg
    assert "--tune" in command_arg

    # Verify the response message
    assert "Successfully triggered training" in response.status_message
    assert response.task_id is not None

    # Verify that no gRPC error was set
    context.set_code.assert_not_called()

def test_trigger_task_unknown_task(servicer):
    """
    Test the handling of an unknown task name.
    """
    # --- Mock Setup ---
    request = orchestrator_pb2.TriggerRequest(task_name="unknown_task")
    context = MagicMock()

    # --- Test Execution ---
    servicer.TriggerTask(request, context)

    # --- Assertions ---
    # Verify that a gRPC error was set
    context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)
    context.set_details.assert_called_once_with("Unknown task name: unknown_task")
