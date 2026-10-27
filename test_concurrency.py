
import asyncio
import httpx


async def create_job(client, number):
    response = await client.post(
        "http://127.0.0.1:8002/jobs",
        json={
            "task": f"Test concurrent job {number}",
            "simulate_failure": False
        }
    )

    print(f"Job {number}: {response.status_code} - {response.json()}")


async def main():
    async with httpx.AsyncClient() as client:
        await asyncio.gather(
            create_job(client, 1),
            create_job(client, 2),
            create_job(client, 3)
        )


asyncio.run(main())
