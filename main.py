"""
main.py
-------
FastAPI application — the backend server.

Endpoints:
  GET  /           → Serve the HTML frontend
  POST /run        → Start the pipeline and stream SSE results
  GET  /health     → Health check

Run with:
  uvicorn main:app --reload --port 8000
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from core.pipeline import run_pipeline
import os

app = FastAPI(
    title="Multi-Agent Research Pipeline",
    description="Agentic AI system for research and report generation",
    version="1.0.0"
)

# Allow frontend to call the API (needed for browser requests)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


class TaskRequest(BaseModel):
    """Request body for starting the pipeline."""
    topic: str


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the HTML frontend."""
    frontend_path = os.path.join(os.path.dirname(__file__), "frontend", "index.html")
    with open(frontend_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


@app.post("/run")
async def run_task(request: TaskRequest):
    """
    Start the multi-agent pipeline for the given topic.
    
    Returns a Server-Sent Events stream so the frontend
    receives updates in real time as each agent completes.
    """
    if not request.topic or len(request.topic.strip()) < 3:
        return {"error": "Please provide a valid topic (at least 3 characters)"}

    topic = request.topic.strip()

    # Return a streaming response — FastAPI handles SSE natively
    return StreamingResponse(
        run_pipeline(topic),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"  # Disable nginx buffering for SSE
        }
    )


@app.get("/health")
async def health_check():
    """Simple health check to verify the server is running."""
    return {
        "status": "healthy",
        "agents": ["Retriever", "Analyzer", "Writer", "Reviewer"],
        "pipeline": "ready"
    }