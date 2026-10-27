
import asyncio
from queue_manager import job_queue
from jobs import jobs


async def process_job(job_id: int):
    jobs[job_id]["status"] = "Running"
    print(f"Job {job_id}: Starting...")

    await asyncio.sleep(5)

    jobs[job_id]["status"] = "Completed"
    print(f"Job {job_id}: Completed!")


async def worker(worker_id: int):
    while True:
        job_id = await job_queue.get()

        try:
            print(f"Worker {worker_id} picked up job {job_id}")
            await process_job(job_id)
        finally:
            job_queue.task_done()


async def main():
    workers = [
        asyncio.create_task(worker(1)),
        asyncio.create_task(worker(2)),
        asyncio.create_task(worker(3))
    ]

    print("AsyncFlow workers are running...")

    try:
        await asyncio.gather(*workers)
    finally:
        for task in workers:
            task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
