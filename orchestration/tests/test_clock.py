import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock

# Add project root to path to allow importing from other services
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from orchestration.clock import trigger_ml_training
from common.generated import orchestrator_pb2

@pytest.mark.asyncio
@patch('orchestration.clock.grpc.aio.insecure_channel')
async def test_trigger_ml_training_success(mock_insecure_channel):
    """
    Test the successful triggering of the ML training task.
    """
    # --- Mock Setup ---
    # Mock the channel and stub
    mock_channel = MagicMock()
    mock_stub = MagicMock()

    # The stub's method needs to be an awaitable mock (AsyncMock)
    mock_stub.TriggerTask = AsyncMock(return_value=orchestrator_pb2.TriggerResponse(
        status_message="Task triggered",
        task_id="test-123"
    ))

    # The channel context manager should return the channel
    mock_insecure_channel.return_value.__aenter__.return_value = mock_channel

    # Patch the stub creation to return our mock stub
    with patch('orchestration.clock.orchestrator_pb2_grpc.OrchestratorStub', return_value=mock_stub):

        # --- Test Execution ---
        response = await trigger_ml_training()

        # --- Assertions ---
        # Assert that a channel was created to the correct address
        mock_insecure_channel.assert_called_once_with('localhost:50051')

        # Assert that the TriggerTask method was called
        mock_stub.TriggerTask.assert_awaited_once()

        # Assert the content of the request passed to TriggerTask
        request_arg = mock_stub.TriggerTask.call_args[0][0]
        assert isinstance(request_arg, orchestrator_pb2.TriggerRequest)
        assert request_arg.task_name == "train_model"
        assert request_arg.parameters["symbol"] == "BTCUSDT"

        # Assert the response from our function
        assert response.status_message == "Task triggered"

@pytest.mark.asyncio
@patch('orchestration.clock.grpc.aio.insecure_channel', new_callable=AsyncMock)
async def test_trigger_ml_training_connection_error(mock_insecure_channel):
    """
    Test the case where the gRPC server is unavailable.
    """
    # --- Mock Setup ---
    # Make the channel context manager raise an AioRpcError
    mock_insecure_channel.side_effect = grpc.aio.AioRpcError(
        grpc.StatusCode.UNAVAILABLE, "Connection failed"
    )

    # --- Test Execution & Assertions ---
    # We expect the function to handle the exception gracefully and return None
    response = await trigger_ml_training()
    assert response is None
