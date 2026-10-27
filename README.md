# AsyncFlow — Job Processing & Monitoring API

AsyncFlow is a Python backend project that demonstrates asynchronous job processing, concurrent workers, automatic retries, job status tracking, and API monitoring.

## Features

- REST API built with FastAPI.
- Asynchronous processing using Python `asyncio`.
- Three background workers for concurrent job processing.
- Job creation and status tracking.
- Automatic retries for simulated failures.
- Cancellation of jobs that are still queued.
- Job statistics and health-check endpoints.
- Structured logging and timestamps.
- Interactive API documentation using Swagger UI.

## Tech Stack

- Python
- FastAPI
- Uvicorn
- asyncio
- Pydantic
- HTTPX

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/jobs` | Create a job |
| GET | `/jobs/{job_id}` | Get job details |
| DELETE | `/jobs/{job_id}` | Cancel a queued job |
| GET | `/stats` | View job statistics |
| GET | `/health` | Check API health |

## Run Locally

### 1. Clone the repository

```bash
git clone YOUR_GITHUB_REPOSITORY_URL
cd AsyncFlow
```

### 2. Create and activate a virtual environment

Windows PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 3. Install dependencies

Create a `requirements.txt` file with the project's required packages, then run:

```powershell
pip install -r requirements.txt
```

### 4. Start the API

```powershell
python -m uvicorn jobs:app --port 8002 --log-level info
```

### 5. Open Swagger UI

Visit http://127.0.0.1:8002/docs

## Current Limitations

- Jobs and queue data are stored in memory and are lost when the server restarts.
- This prototype runs workers within one application process.
- Failures are simulated for testing retry behavior.
- Production deployment would require durable queueing, persistent storage, and additional monitoring.

## Learning Objectives

This project demonstrates practical experience with asynchronous Python, REST API design, concurrent task processing, retry logic, error handling, and structured logging.
