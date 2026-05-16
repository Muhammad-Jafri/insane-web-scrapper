import asyncio

from app.logging_config import setup_logging
from app.worker.runner import run_worker

if __name__ == "__main__":
    setup_logging()
    asyncio.run(run_worker())
