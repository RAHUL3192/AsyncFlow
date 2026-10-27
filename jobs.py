
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from itertools import count

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from queue_manager import job_queue
from logging_config import setup_logging


# Configure logging
setup_logging()
logger = logging.getLogger("asyncflow")

# In-memory job storage
jobs = {}
job_ids = count(101)

# Maximum total attempts per job
MAX_ATTEMPTS = 3


def current_time():
    return datetime.now(timezone.utc).isoformat()


# Validate incoming job requests
class JobRequest(BaseModel):
    task: str = Field(
        default="Generate report",
        min_length=3,
        max_length=100
    )
    simulate_failure: bool = False


# Background worker
async def worker(worker_id: int):
    while True:
        job_id = await job_queue.get()

        try:
            if job_id not in jobs:
                continue

            job = jobs[job_id]

            # Ignore jobs that were cancelled while queued
            if job["status"] != "Queued":
                continue

            job["status"] = "Running"

            if job["started_at"] is None:
                job["started_at"] = current_time()

            logger.info(
                "Worker %s started job %s",
                worker_id,
                job_id
            )

            try:
                # Simulate task processing
                await asyncio.sleep(5)

                # Simulate a failure when requested
                if job["simulate_failure"]:
                    raise RuntimeError("Simulated job failure")

                job["status"] = "Completed"
                job["completed_at"] = current_time()
                job["error"] = None

                logger.info(
                    "Job %s completed successfully",
                    job_id
                )

            except Exception as error:
                job["retries"] += 1
                job["error"] = str(error)

                logger.exception(
                    "Job %s failed on attempt %s",
                    job_id,
                    job["retries"]
                )

                if job["retries"] < MAX_ATTEMPTS:
                    job["status"] = "Queued"
                    await job_queue.put(job_id)

                    logger.warning(
                        "Retrying job %s",
                        job_id
                    )
                else:
                    job["status"] = "Failed"
                    job["completed_at"] = current_time()

                    logger.error(
                        "Job %s failed after %s attempts",
                        job_id,
                        MAX_ATTEMPTS
                    )

        finally:
            job_queue.task_done()


# Start and stop workers with the application
@asynccontextmanager
async def lifespan(app: FastAPI):
    workers = [
        asyncio.create_task(worker(1)),
        asyncio.create_task(worker(2)),
        asyncio.create_task(worker(3))
    ]

    logger.info("AsyncFlow started with 3 workers")

    try:
        yield
    finally:
        for task in workers:
            task.cancel()

        await asyncio.gather(
            *workers,
            return_exceptions=True
        )

        logger.info("AsyncFlow workers stopped")


# FastAPI application
app = FastAPI(
    title="AsyncFlow Job Manager",
    description=(
        "An asynchronous job processing "
        "and monitoring API"
    ),
    version="3.0.0",
    lifespan=lifespan
)


# Create a job
@app.post("/jobs")
async def create_job(request: JobRequest):
    job_id = next(job_ids)

    job = {
        "job_id": job_id,
        "task": request.task,
        "status": "Queued",
        "retries": 0,
        "simulate_failure": request.simulate_failure,
        "error": None,
        "created_at": current_time(),
        "started_at": None,
        "completed_at": None
    }

    jobs[job_id] = job
    await job_queue.put(job_id)

    logger.info("Created job %s", job_id)

    return job


# Get a specific job
@app.get("/jobs/{job_id}")
async def get_job(job_id: int):
    if job_id not in jobs:
        logger.warning(
            "Job %s was not found",
            job_id
        )

        raise HTTPException(
            status_code=404,
            detail="Job not found"
        )

    return jobs[job_id]


# Get job statistics
@app.get("/stats")
async def get_stats():
    stats = {
        "total_jobs": len(jobs),
        "queued": 0,
        "running": 0,
        "completed": 0,
        "failed": 0,
        "cancelled": 0
    }

    for job in jobs.values():
        status = job["status"].lower()

        if status in stats:
            stats[status] += 1

    logger.info("Job statistics requested")

    return stats


# Health-check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "AsyncFlow Job Manager",
        "workers": 3
    }


# Cancel a queued job
@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: int):
    if job_id not in jobs:
        raise HTTPException(
            status_code=404,
            detail="Job not found"
        )

    job = jobs[job_id]

    if job["status"] != "Queued":
        raise HTTPException(
            status_code=400,
            detail=(
                "Only queued jobs can be cancelled"
            )
        )

    job["status"] = "Cancelled"
    job["completed_at"] = current_time()

    logger.info("Job %s was cancelled", job_id)

    return {
        "message": "Job cancelled successfully",
        "job_id": job_id,
        "status": job["status"]
    }
