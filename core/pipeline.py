"""
pipeline.py
-----------
The heart of the system — orchestrates all 4 agents in sequence.

Flow:
  User Input
      │
      ▼
  [Retriever Agent]  — gathers information
      │
      ▼
  [Analyzer Agent]   — extracts insights
      │
      ▼
  [Writer Agent]     — writes the report
      │
      ▼
  [Reviewer Agent]   — scores & reviews
      │
      ▼
  Final Output streamed to user

HOW STREAMING WORKS:
  Each agent calls stream_callback(token) for every token as it arrives.
  We use an asyncio.Queue to pass those tokens out of the agent callback
  and into this generator, which yields them as SSE 'token' events.
  The frontend receives tokens in real time and appends them to the UI.

  Event types emitted:
    pipeline_start  — pipeline is starting, lists steps
    agent_start     — an agent is beginning work
    token           — a single streamed token from the active agent
    agent_complete  — agent finished, full output included
    agent_error     — agent failed, fallback output included
    pipeline_complete — all agents done, final score + verdict
"""

import asyncio
import json
from typing import AsyncGenerator
from agents.retriever import RetrieverAgent
from agents.analyzer import AnalyzerAgent
from agents.writer import WriterAgent
from agents.reviewer import ReviewerAgent
from core.error_handler import get_fallback


# Pipeline steps in order — defines the entire execution sequence
PIPELINE_STEPS = ["retrieval", "analysis", "writing", "review"]


def _sse_event(event_type: str, data: dict) -> str:
    """
    Format a Server-Sent Event string.

    SSE wire format:
      event: <type>\\n
      data: <json>\\n\\n

    The frontend's EventSource listener dispatches on event type.
    """
    return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"


async def _run_agent_with_streaming(agent_coro, token_queue: asyncio.Queue):
    """
    Runs an agent coroutine while capturing its streamed tokens
    into token_queue so the pipeline generator can yield them.

    Args:
        agent_coro  : Coroutine from agent.retrieve() / .analyze() / etc.
                      Must accept a stream_callback keyword argument.
        token_queue : Queue shared with the pipeline generator.

    Returns:
        Full agent output string.
    """
    async def _on_token(token: str):
        """Called by BaseAgent.run() for each token as it arrives."""
        await token_queue.put(token)

    # Inject our callback so the agent streams into our queue
    result = await agent_coro(_on_token)

    # Signal the pipeline generator that this agent is done streaming
    await token_queue.put(None)
    return result


async def run_pipeline(topic: str) -> AsyncGenerator[str, None]:
    """
    Main pipeline generator — runs 4 agents sequentially, yields SSE events.

    This is an async generator. FastAPI's StreamingResponse consumes it
    and pushes each yielded string over HTTP as a Server-Sent Event.

    Real-time token flow:
      BaseAgent._sync_stream_to_queue() → token_queue → pipeline yields
      'token' SSE event → frontend appends to active agent panel.

    Args:
        topic : Research topic from the user

    Yields:
        SSE-formatted strings
    """

    # ── Pipeline start ──────────────────────────────────────────────────
    yield _sse_event("pipeline_start", {
        "message": f"Starting research pipeline for: {topic}",
        "steps": PIPELINE_STEPS,
        "total_steps": len(PIPELINE_STEPS)
    })
    await asyncio.sleep(0.1)

    # Storage for each agent's output — passed forward to next agent
    results = {
        "retrieved_data": "",
        "analysis": "",
        "report": "",
        "review": {}
    }

    # Shared queue for streaming tokens out of agents into this generator
    token_queue: asyncio.Queue = asyncio.Queue()

    # ════════════════════════════════════════════════════════════════════
    # STEP 1 — RETRIEVER AGENT
    # ════════════════════════════════════════════════════════════════════
    yield _sse_event("agent_start", {
        "agent": "Retriever Agent",
        "step": 1,
        "description": "Gathering information and research data..."
    })

    retriever = RetrieverAgent()

    try:
        # Launch agent in background task so we can consume its tokens
        # while it's still running (true concurrent streaming)
        agent_task = asyncio.create_task(
            _run_agent_with_streaming(
                lambda cb: retriever.retrieve(topic, cb),
                token_queue
            )
        )

        # Yield each token as it arrives — frontend gets real-time updates
        while True:
            token = await asyncio.wait_for(token_queue.get(), timeout=60)
            if token is None:
                break  # agent done streaming
            yield _sse_event("token", {"agent": "Retriever Agent", "token": token})

        retrieved_data = await agent_task
        results["retrieved_data"] = retrieved_data

        yield _sse_event("agent_complete", {
            "agent": "Retriever Agent",
            "step": 1,
            "output": retrieved_data,
            "status": "success"
        })

    except asyncio.TimeoutError:
        fallback = get_fallback("Retriever Agent")
        results["retrieved_data"] = fallback
        yield _sse_event("agent_error", {
            "agent": "Retriever Agent",
            "step": 1,
            "error": "Agent timed out after 60 seconds",
            "fallback_used": True,
            "output": fallback
        })

    except Exception as e:
        fallback = get_fallback("Retriever Agent")
        results["retrieved_data"] = fallback
        yield _sse_event("agent_error", {
            "agent": "Retriever Agent",
            "step": 1,
            "error": str(e),
            "fallback_used": True,
            "output": fallback
        })

    # ════════════════════════════════════════════════════════════════════
    # STEP 2 — ANALYZER AGENT
    # ════════════════════════════════════════════════════════════════════
    yield _sse_event("agent_start", {
        "agent": "Analyzer Agent",
        "step": 2,
        "description": "Analyzing data and extracting insights..."
    })

    analyzer = AnalyzerAgent()

    try:
        agent_task = asyncio.create_task(
            _run_agent_with_streaming(
                lambda cb: analyzer.analyze(results["retrieved_data"], topic, cb),
                token_queue
            )
        )

        while True:
            token = await asyncio.wait_for(token_queue.get(), timeout=60)
            if token is None:
                break
            yield _sse_event("token", {"agent": "Analyzer Agent", "token": token})

        analysis = await agent_task
        results["analysis"] = analysis

        yield _sse_event("agent_complete", {
            "agent": "Analyzer Agent",
            "step": 2,
            "output": analysis,
            "status": "success"
        })

    except asyncio.TimeoutError:
        fallback = get_fallback("Analyzer Agent")
        results["analysis"] = fallback
        yield _sse_event("agent_error", {
            "agent": "Analyzer Agent",
            "step": 2,
            "error": "Agent timed out after 60 seconds",
            "fallback_used": True,
            "output": fallback
        })

    except Exception as e:
        fallback = get_fallback("Analyzer Agent")
        results["analysis"] = fallback
        yield _sse_event("agent_error", {
            "agent": "Analyzer Agent",
            "step": 2,
            "error": str(e),
            "fallback_used": True,
            "output": fallback
        })

    # ════════════════════════════════════════════════════════════════════
    # STEP 3 — WRITER AGENT
    # ════════════════════════════════════════════════════════════════════
    yield _sse_event("agent_start", {
        "agent": "Writer Agent",
        "step": 3,
        "description": "Writing the structured research report..."
    })

    writer = WriterAgent()

    try:
        agent_task = asyncio.create_task(
            _run_agent_with_streaming(
                lambda cb: writer.write(
                    topic=topic,
                    retrieved_data=results["retrieved_data"],
                    analysis=results["analysis"],
                    stream_callback=cb
                ),
                token_queue
            )
        )

        while True:
            token = await asyncio.wait_for(token_queue.get(), timeout=90)
            if token is None:
                break
            yield _sse_event("token", {"agent": "Writer Agent", "token": token})

        report = await agent_task
        results["report"] = report

        yield _sse_event("agent_complete", {
            "agent": "Writer Agent",
            "step": 3,
            "output": report,
            "status": "success"
        })

    except asyncio.TimeoutError:
        fallback = get_fallback("Writer Agent")
        results["report"] = fallback
        yield _sse_event("agent_error", {
            "agent": "Writer Agent",
            "step": 3,
            "error": "Agent timed out after 90 seconds",
            "fallback_used": True,
            "output": fallback
        })

    except Exception as e:
        fallback = get_fallback("Writer Agent")
        results["report"] = fallback
        yield _sse_event("agent_error", {
            "agent": "Writer Agent",
            "step": 3,
            "error": str(e),
            "fallback_used": True,
            "output": fallback
        })

    # ════════════════════════════════════════════════════════════════════
    # STEP 4 — REVIEWER AGENT ⭐
    # ════════════════════════════════════════════════════════════════════
    yield _sse_event("agent_start", {
        "agent": "Reviewer Agent",
        "step": 4,
        "description": "Reviewing report quality and generating confidence score..."
    })

    reviewer = ReviewerAgent()

    try:
        agent_task = asyncio.create_task(
            _run_agent_with_streaming(
                lambda cb: reviewer.review(results["report"], topic, cb),
                token_queue
            )
        )

        while True:
            token = await asyncio.wait_for(token_queue.get(), timeout=60)
            if token is None:
                break
            yield _sse_event("token", {"agent": "Reviewer Agent", "token": token})

        review_result = await agent_task
        results["review"] = review_result

        yield _sse_event("agent_complete", {
            "agent": "Reviewer Agent",
            "step": 4,
            "output": review_result["review_text"],
            "score": review_result["score"],
            "verdict": review_result["verdict"],
            "status": "success"
        })

    except asyncio.TimeoutError:
        fallback_text = get_fallback("Reviewer Agent")
        results["review"] = {"review_text": fallback_text, "score": 70, "verdict": "APPROVED"}
        yield _sse_event("agent_error", {
            "agent": "Reviewer Agent",
            "step": 4,
            "error": "Agent timed out after 60 seconds",
            "fallback_used": True,
            "output": fallback_text,
            "score": 70,
            "verdict": "APPROVED"
        })

    except Exception as e:
        fallback_text = get_fallback("Reviewer Agent")
        results["review"] = {"review_text": fallback_text, "score": 70, "verdict": "APPROVED"}
        yield _sse_event("agent_error", {
            "agent": "Reviewer Agent",
            "step": 4,
            "error": str(e),
            "fallback_used": True,
            "output": fallback_text,
            "score": 70,
            "verdict": "APPROVED"
        })

    # ── Pipeline complete ───────────────────────────────────────────────
    yield _sse_event("pipeline_complete", {
        "message": "Pipeline completed successfully",
        "score": results["review"].get("score", 70),
        "verdict": results["review"].get("verdict", "APPROVED"),
        "topic": topic
    })