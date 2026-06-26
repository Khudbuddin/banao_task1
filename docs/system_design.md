# System Design Document
## Multi-Agent Research Pipeline — Banao Technologies Task 1

---

## 1. Overview

This system accepts a complex research topic from the user, decomposes it into 4 discrete steps, assigns each step to a specialized AI agent, streams tokens in real time to the frontend, and handles failures gracefully using fallback responses. The final output is presented both as a live pipeline view (for transparency) and as a clean formatted report (for usability).

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      USER BROWSER                            │
│  [ Input: Research Topic ]                                   │
│  [ Tab 1: Pipeline View — 4 live agent panels ]             │
│  [ Tab 2: Final Report — clean formatted output ]           │
└──────────────────────┬──────────────────────────────────────┘
                       │  HTTP POST /run  (SSE stream back)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  FASTAPI SERVER (main.py)                    │
│  - Receives topic from user                                  │
│  - Calls run_pipeline() async generator                      │
│  - Returns StreamingResponse (text/event-stream)            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           PIPELINE ORCHESTRATOR (core/pipeline.py)           │
│                                                              │
│  Step 1        Step 2        Step 3        Step 4            │
│ ┌─────────┐  ┌──────────┐  ┌────────┐  ┌──────────┐        │
│ │Retriever│→ │ Analyzer │→ │ Writer │→ │ Reviewer │        │
│ │  Agent  │  │  Agent   │  │ Agent  │  │  Agent   │        │
│ └─────────┘  └──────────┘  └────────┘  └──────────┘        │
│      │             │            │             │              │
│  [fallback]    [fallback]   [fallback]   [fallback]         │
│                                                              │
│  asyncio.Queue bridges sync Groq SDK → async SSE stream     │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  GROQ API (Free Tier)                        │
│  Model: llama-3.3-70b-versatile                             │
│  - Each agent calls Groq with its own system prompt         │
│  - Direct API calls — zero framework abstraction            │
│  - True token-by-token streaming via stream=True            │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. How TRUE Token Streaming Works

This is the most technically significant part of the system.

### The Problem
The Groq SDK is **synchronous** — it blocks the thread while streaming. But our pipeline is **async** (FastAPI + asyncio). Mixing sync blocking code with async code incorrectly would freeze the entire server.

### The Solution — asyncio.Queue Bridge

```
Groq API (sync stream)
     │
     │  chunk by chunk
     ▼
ThreadPoolExecutor (background thread)
     │
     │  loop.call_soon_threadsafe(queue.put_nowait, token)
     ▼
asyncio.Queue  ◄── thread-safe bridge
     │
     │  await queue.get()
     ▼
Pipeline async generator
     │
     │  yield SSE 'token' event
     ▼
Frontend (token appended to active agent panel)
```

### Why This Matters
- Tokens arrive at the frontend **one by one** as the model generates them
- The async event loop is **never blocked** — other requests can be served concurrently
- The sentinel pattern (`None` in queue) cleanly signals end-of-stream
- Exceptions from the sync thread are pushed into the queue and re-raised on the async side

### Code Reference
```python
# base_agent.py — inside _sync_stream_to_queue()
loop.call_soon_threadsafe(token_queue.put_nowait, token)

# pipeline.py — consuming tokens
while True:
    token = await asyncio.wait_for(token_queue.get(), timeout=60)
    if token is None:
        break
    yield _sse_event("token", {"agent": agent_name, "token": token})
```

---

## 4. Component Breakdown

### 4.1 BaseAgent (agents/base_agent.py)
- Parent class for all 4 agents
- Holds: name, system_prompt, Groq API client, model name
- Key method: `run(user_input, stream_callback)` 
- Streaming: asyncio.Queue bridge (sync SDK → async pipeline)
- Uses `asyncio.get_running_loop()` (correct Python 3.10+ API)
- Retry strategy: exponential backoff for connection errors, longer waits for rate limits, immediate raise for auth errors

### 4.2 RetrieverAgent (agents/retriever.py)
- System prompt: Research specialist — gathers structured facts
- Input: Raw topic string from user
- Output: Key facts, recent trends, key players, controversies, raw data summary
- Timeout: 60 seconds

### 4.3 AnalyzerAgent (agents/analyzer.py)
- System prompt: Expert analyst — extracts insights from raw data
- Input: Retrieved data + original topic
- Output: Ranked insights (HIGH/MEDIUM/LOW), patterns, surprising findings, impact assessment, gaps
- Timeout: 60 seconds

### 4.4 WriterAgent (agents/writer.py)
- System prompt: Professional report writer
- Input: Retrieved data + analysis + topic
- Output: Full markdown report (Executive Summary → Recommendations → Conclusion)
- Timeout: 90 seconds (longer — generates more content)
- Output also populates the Final Report tab in the frontend

### 4.5 ReviewerAgent (agents/reviewer.py)
- System prompt: Senior quality reviewer
- Input: Generated report + topic
- Output: Scored review (0–100 across 5 dimensions) + verdict (APPROVED / NEEDS REVISION)
- Score extracted via regex from response text
- Score displayed as confidence badge in the UI

### 4.6 Pipeline Orchestrator (core/pipeline.py)
- Async generator function: `run_pipeline(topic)`
- Runs agents sequentially: Retriever → Analyzer → Writer → Reviewer
- Each agent output passed as input to the next agent
- Uses `asyncio.create_task()` to run agent while consuming its token queue
- Yields SSE events: `pipeline_start`, `agent_start`, `token`, `agent_complete`, `agent_error`, `pipeline_complete`
- On any agent failure: uses fallback response, pipeline continues

### 4.7 Error Handler (core/error_handler.py)
- Custom exceptions: `AgentFailureError`, `PipelineError`, `TimeoutError`
- Pre-written fallback responses for each agent
- Guarantees pipeline always produces some output — never crashes completely

### 4.8 FastAPI Server (main.py)
- `GET /` → Serves HTML frontend (UTF-8 encoding explicit)
- `POST /run` → Starts pipeline, returns `StreamingResponse` with `text/event-stream`
- `GET /health` → Health check endpoint
- CORS middleware enabled for browser access

### 4.9 Frontend (frontend/index.html)
- Single HTML file — zero build step, instant demo
- **Tab 1 — Pipeline View:** 4 agent panels with live token streaming, step tracker with progress bar, fallback error banners
- **Tab 2 — Final Report:** Clean markdown-rendered report from Writer Agent, copy-to-clipboard button
- Score card: circular score display + APPROVED/NEEDS REVISION verdict badge
- Example topic chips for quick demo
- No reload needed between searches — full state reset on each run

---

## 5. SSE Event Types

| Event | When fired | Key data |
|---|---|---|
| `pipeline_start` | Pipeline begins | topic, steps list |
| `agent_start` | Agent begins work | agent name, step number |
| `token` | Each token arrives | agent name, token string |
| `agent_complete` | Agent finishes | agent name, full output, step |
| `agent_error` | Agent fails | error message, fallback output |
| `pipeline_complete` | All agents done | score, verdict, topic |

---

## 6. Data Flow

```
User Input: "Impact of AI on the job market"
     │
     ▼
RetrieverAgent.retrieve(topic)
     │ → streams tokens live to frontend
     │ Output: Structured research (facts, trends, players)
     ▼
AnalyzerAgent.analyze(retrieved_data, topic)
     │ → streams tokens live to frontend
     │ Output: Ranked insights, patterns, impact assessment
     ▼
WriterAgent.write(topic, retrieved_data, analysis)
     │ → streams tokens live to frontend
     │ Output: Full markdown report → also shown in Final Report tab
     ▼
ReviewerAgent.review(report, topic)
     │ → streams tokens live to frontend
     │ Output: { review_text, score: 84, verdict: "APPROVED" }
     ▼
pipeline_complete SSE → frontend shows final score badge
```

---

## 7. Failure Handling

| Failure Type | Detection | Response |
|---|---|---|
| API connection error | `"connection" in error_msg` | Retry 3x, exponential backoff (2s/4s/8s) |
| Rate limit (429) | `"rate limit" in error_msg` | Retry with longer delay (5s/10s/15s) |
| Timeout | `asyncio.wait_for()` exception | Use fallback, continue pipeline |
| Auth error (401) | `"auth" in error_msg` | Surface immediately, no retry |
| Any agent crash | `except Exception` | Use fallback response, pipeline continues |
| Thread exception | Exception pushed into queue | Re-raised on async side, caught by pipeline |

**Key principle:** No single agent failure stops the pipeline. Every step has a meaningful fallback.

---

## 8. Technology Choices

| Technology | Reason |
|---|---|
| Python 3.10+ | Native asyncio, correct `get_running_loop()` API |
| FastAPI | Native async, built-in SSE via StreamingResponse |
| asyncio + Queue | Non-blocking pipeline, thread-safe token bridge |
| Groq API (llama-3.3-70b) | Best free tier available, fast inference |
| Direct API calls | Full transparency — no hidden orchestration |
| SSE over WebSockets | Simpler for one-directional streaming, no handshake |
| Single HTML file | Zero build step, instant demo, easy to share |

---

## 9. What We Explicitly Avoided

- **LangChain / AutoGen / CrewAI** — These hide agent orchestration, retry logic, and data flow behind abstractions. We wrote every behavior explicitly so it is fully auditable and understandable.
- **Black-box prompt chaining** — Every system prompt is clearly defined, separated by concern, and independently readable.
- **Fake streaming** — We did not collect the full response and send it at the end. Real token-by-token streaming via asyncio.Queue bridge proves the system works as advertised.
- **Single monolithic prompt** — Decomposed into 4 specialized agents each with a focused, distinct job and system prompt.