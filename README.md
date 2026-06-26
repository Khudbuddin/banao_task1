# Multi-Agent Research Pipeline
### Banao Technologies — Task 1 Submission

A production-style agentic AI system that accepts a complex research topic, decomposes it into discrete steps, assigns each step to a specialized agent, streams partial results **token by token** in real time, and handles failures gracefully.

---

## Architecture

```
User Input → [Retriever] → [Analyzer] → [Writer] → [Reviewer] → Streamed Output
```

4 specialized agents, each with a distinct system prompt and responsibility:

| Agent | Job |
|---|---|
| 🔍 Retriever | Gathers facts, statistics, trends, key players |
| 🧠 Analyzer | Extracts insights, patterns, and implications |
| ✍️ Writer | Writes a structured professional report |
| ⭐ Reviewer | Scores quality (0–100) and flags weak sections |

**No LangChain. No AutoGen. No CrewAI.** Every agent, pipeline step, retry, and fallback is written explicitly.

**AI Provider: Groq (free tier) — Llama 3.3 70B**  
Groq's free API provides fast inference. The agent layer is provider-agnostic — swapping to OpenAI or Anthropic requires changing only `BaseAgent.__init__`.

---

## How Real Streaming Works

Most tutorials fake streaming by sending the full response at the end. This system streams **token by token**:

```
Groq API (sync stream)
  └─► _sync_stream_to_queue()  [runs in ThreadPoolExecutor]
        └─► loop.call_soon_threadsafe(queue.put, token)
              └─► pipeline generator reads queue
                    └─► yields SSE 'token' event
                          └─► frontend appends token to UI
```

The `asyncio.Queue` bridges the synchronous Groq SDK and the async pipeline without blocking the event loop.

---

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/banao-task1.git
cd banao-task1
```

### 2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Add your API key
```bash
cp .env.example .env
# Edit .env — add your Groq API key
```

Get a free Groq API key at: **https://console.groq.com**  
No credit card required. Free tier is sufficient for this project.

Your `.env` should contain:
```
GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxxxxxx
```

### 5. Run the server
```bash
uvicorn main:app --reload --port 8000
```

### 6. Open the app
Go to: **http://localhost:8000**

---

## Project Structure

```
banao-task1/
├── agents/
│   ├── __init__.py
│   ├── base_agent.py      # Base class: Groq API calls, streaming, retry logic
│   ├── retriever.py       # Agent 1: Research & data gathering
│   ├── analyzer.py        # Agent 2: Insight extraction
│   ├── writer.py          # Agent 3: Report generation
│   └── reviewer.py        # Agent 4: Quality scoring
├── core/
│   ├── __init__.py
│   ├── pipeline.py        # Async pipeline orchestrator + SSE streaming
│   └── error_handler.py   # Custom exceptions + fallback responses
├── frontend/
│   └── index.html         # Single-file streaming UI
├── docs/
│   ├── system_design.md   # Architecture & data flow
│   └── post_mortem.md     # Scaling issues, trade-offs, design decisions
├── main.py                # FastAPI server
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Key Features

- **Real token-by-token streaming** via `asyncio.Queue` + Server-Sent Events
- **Graceful failure handling** — any agent can fail; pipeline continues with fallback
- **Confidence scoring** — Reviewer Agent scores the report 0–100
- **Async pipeline** using Python `asyncio` + `ThreadPoolExecutor`
- **Retry logic** with exponential backoff (2s → 4s → 8s)
- **Zero black-box frameworks** — all orchestration written from scratch

---

## Demonstrating a Failure Case

To simulate an agent failure for the demo video:

1. In `agents/analyzer.py`, add this at the top of `analyze()`:
```python
raise Exception("Simulated Analyzer failure for demo")
```
2. Run the pipeline — the Analyzer fails, fallback activates, Writer and Reviewer still run
3. The UI shows a red "⚠ Fallback used" badge for that step
4. Remove the line after recording

---

## Documents

- [System Design Document](docs/system_design.md)
- [Post-Mortem Document](docs/post_mortem.md)

---

*Built for Banao Technologies Internship — Task 1*