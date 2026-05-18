import asyncio
import socket

from app.logging_config import setup_logging
from app.worker.runner import run_worker

if __name__ == "__main__":
    setup_logging(log_dir="logs/worker", filename=f"{socket.gethostname()}.log")
    asyncio.run(run_worker())
