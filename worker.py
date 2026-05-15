import asyncio

from app.worker.runner import run_worker

if __name__ == "__main__":
    asyncio.run(run_worker())
