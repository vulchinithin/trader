import asyncio
from prefect import task, flow
import sys
import os

# --- Path Setup ---
# This allows the script to be run directly and find the other modules
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

async def run_process(command: str):
    """Helper function to run a command and stream its output."""
    print(f"--- Running command: {command} ---")
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # Stream stdout and stderr
    async def log_stream(stream, logger):
        while True:
            line = await stream.readline()
            if line:
                logger(line.decode().strip())
            else:
                break

    # It's important to use a logger or print function passed from the task
    # For now, we'll just use print
    await asyncio.gather(
        log_stream(process.stdout, print),
        log_stream(process.stderr, print)
    )

    await process.wait()
    print(f"--- Command finished with exit code {process.returncode} ---")
    if process.returncode != 0:
        raise Exception(f"Command failed: {command}")
    return process

@task
async def run_training_task(symbol: str, tune: bool = False):
    """A Prefect task to run the training pipeline."""
    model_type = "xgboost_regressor"
    tune_flag = "--tune" if tune else ""
    command = (
        f"python -m ml_training.train --symbol {symbol} "
        f"--model-type {model_type} {tune_flag}"
    )
    await run_process(command)

@task
async def run_model_selection_task(symbol: str):
    """A Prefect task to run the model selection pipeline."""
    model_base_name = f"{symbol}_xgboost_regressor"
    command = (
        f"python -m model_selection.selector --symbol {symbol} "
        f"--model-base-name {model_base_name}"
    )
    await run_process(command)

# --- Main Flow ---
# The other services (ingestion, feature generation, etc.) are long-running
# and would typically be managed as deployments in a real Prefect setup.
# For a simple script-based orchestration, we will focus on the batch jobs.

@flow(name="Train and Select Best Model")
async def train_and_select_flow(symbol: str = "BTCUSDT"):
    """
    A Prefect flow that orchestrates the training and selection of a model.
    """
    print("--- Starting Train and Select Flow ---")

    # Run the training task (with tuning enabled)
    training_run = await run_training_task.submit(symbol=symbol, tune=True)

    # Run the model selection task, which depends on the training being complete
    selection_run = await run_model_selection_task.submit(
        symbol=symbol,
        wait_for=[training_run]
    )

    print(f"--- Flow finished. Selection task state: {selection_run.get_state().name} ---")

if __name__ == "__main__":
    # To run this flow, you would typically use the Prefect CLI:
    # `prefect server start` in one terminal
    # `python orchestration/main_flow.py` in another to register and run

    # For direct execution and testing:
    asyncio.run(train_and_select_flow("BTCUSDT"))
