import asyncio
import logging
import grpc
import os
from common.logging_setup import setup_logging
from common.generated import orchestrator_pb2, orchestrator_pb2_grpc

# --- Setup Logging ---
setup_logging('orchestration-clock')
logger = logging.getLogger(__name__)

# --- Configuration ---
TICK_INTERVAL_SECONDS = int(os.getenv('CLOCK_TICK_INTERVAL', 300)) # Default to 5 minutes
ML_TRAINING_SERVICE_ADDRESS = os.getenv('ML_TRAINING_SERVICE_GRPC_ADDRESS', 'localhost:50051')

async def trigger_ml_training():
    """
    Acts as a gRPC client to trigger a task in the ML training service.
    """
    logger.info(f"Attempting to trigger ML training task at {ML_TRAINING_SERVICE_ADDRESS}...")
    try:
        async with grpc.aio.insecure_channel(ML_TRAINING_SERVICE_ADDRESS) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorStub(channel)

            request = orchestrator_pb2.TriggerRequest(
                task_name="train_model",
                parameters={"symbol": "BTCUSDT", "tune": "true"}
            )

            response = await stub.TriggerTask(request)

            logger.info(f"Successfully triggered task. Response: {response.status_message}")
            return response

    except grpc.aio.AioRpcError as e:
        logger.error(f"Could not connect to gRPC server at {ML_TRAINING_SERVICE_ADDRESS}: {e.details()}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during gRPC call: {e}", exc_info=True)

async def clock_loop():
    """
    The main loop for the Clock service. Ticks at a regular interval.
    """
    logger.info(f"Clock service started. Ticking every {TICK_INTERVAL_SECONDS} seconds.")
    while True:
        logger.info("--- Clock Tick ---")

        # In a real system, there would be more complex logic here to decide
        # which tasks to run based on time, events, or system state.
        # For now, we will just trigger the ML training task on every tick.

        await trigger_ml_training()

        logger.info(f"--- Tick Complete. Sleeping for {TICK_INTERVAL_SECONDS} seconds. ---")
        await asyncio.sleep(TICK_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        asyncio.run(clock_loop())
    except KeyboardInterrupt:
        logger.info("Clock service stopped by user.")
