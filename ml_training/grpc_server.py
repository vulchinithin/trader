import asyncio
import logging
import grpc
import subprocess
import sys
import os
from concurrent import futures

# --- Path Setup ---
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from common.generated import orchestrator_pb2, orchestrator_pb2_grpc
from common.logging_setup import setup_logging

# --- Setup Logging ---
setup_logging('ml-training-grpc-server')
logger = logging.getLogger(__name__)

# --- gRPC Servicer Implementation ---
class OrchestratorServicer(orchestrator_pb2_grpc.OrchestratorServicer):
    """
    Implements the gRPC service for the ML Training service.
    """
    def TriggerTask(self, request, context):
        """
        Handles the TriggerTask RPC call from the orchestrator.
        """
        logger.info(f"Received TriggerTask request for task: {request.task_name}")

        if request.task_name == "train_model":
            try:
                # Extract parameters
                symbol = request.parameters.get("symbol", "BTCUSDT")
                tune = request.parameters.get("tune", "false").lower() == "true"

                # Construct the command to run the training script
                tune_flag = "--tune" if tune else ""
                command = f"python -m ml_training.train --symbol {symbol} {tune_flag}"

                logger.info(f"Executing training command: {command}")

                # Run the training script as a non-blocking subprocess
                # In a real production system, you might use a job queue (e.g., Celery)
                # or a more robust process management system.
                subprocess.Popen(command, shell=True)

                response_message = f"Successfully triggered training for symbol {symbol}."
                task_id = f"train-{symbol}-{os.getpid()}" # A simple task ID

                return orchestrator_pb2.TriggerResponse(
                    status_message=response_message,
                    task_id=task_id
                )

            except Exception as e:
                error_message = f"Failed to trigger training task: {e}"
                logger.error(error_message, exc_info=True)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(error_message)
                return orchestrator_pb2.TriggerResponse()
        else:
            error_message = f"Unknown task name: {request.task_name}"
            logger.warning(error_message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(error_message)
            return orchestrator_pb2.TriggerResponse()

# --- Server Setup ---
async def serve():
    """
    Starts the gRPC server.
    """
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    orchestrator_pb2_grpc.add_OrchestratorServicer_to_server(OrchestratorServicer(), server)

    port = os.getenv('ML_TRAINING_SERVICE_GRPC_PORT', '50051')
    server.add_insecure_port(f'[::]:{port}')

    logger.info(f"Starting gRPC server on port {port}...")
    await server.start()

    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("gRPC server is shutting down.")
        await server.stop(0)

if __name__ == '__main__':
    asyncio.run(serve())
