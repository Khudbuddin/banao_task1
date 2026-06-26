# Post-Mortem Document
## Multi-Agent Research Pipeline — Banao Technologies Task 1

---

## 1. Scaling Issue I Encountered / Anticipate

### Issue: Sequential Pipeline Bottleneck Under Concurrent Load

**What happens:**
The pipeline runs agents sequentially — Retriever → Analyzer → Writer → Reviewer. Each agent waits for the previous one to complete before starting. For a single user this works fine (~2–3 minutes total). But under concurrent load this becomes a serious bottleneck.

**The numbers:**
- Average agent response time: ~25–40 seconds per agent
- Total pipeline time per request: ~2–3 minutes
- With 50 concurrent users: requests queue up, some users wait 10+ minutes
- Groq free tier rate limits compound this — multiple pipeline runs hitting rate limits simultaneously

**Root cause:**
FastAPI handles concurrent HTTP requests well, but each pipeline run makes 4 sequential Groq API calls. External API latency multiplied by 4 agents, multiplied by concurrent users = severe queuing.

**Additionally:** The asyncio.Queue bridge uses `run_in_executor` which draws from a shared ThreadPoolExecutor. Under high load, the thread pool can become exhausted.

**How I would solve it at scale:**
1. **Task queue architecture** — Use Celery + Redis to queue pipeline jobs. Users receive a job ID immediately and poll for results, removing the need to hold open HTTP connections.
2. **Parallel enrichment step** — After retrieval, run a "statistics fetcher" and "examples fetcher" simultaneously using `asyncio.gather()`, feeding both into the Analyzer.
3. **Response caching** — Cache retrieved data for identical or semantically similar topics using vector similarity. Avoid redundant retrieval for common queries.
4. **Dedicated thread pool** — Use a custom `ThreadPoolExecutor` with a controlled size instead of the default executor, preventing thread pool exhaustion.

---

## 2. Design Change I Would Make in Hindsight

### Change: Separate Streaming Transport from Pipeline Logic

**What I built:**
`pipeline.py` does two things at once — it executes the agents AND formats SSE events. Business logic and transport logic are mixed in the same file.

**Why this is a problem:**
- Cannot unit test the pipeline without a running HTTP server
- Switching from SSE to WebSockets requires rewriting `pipeline.py`
- Adding logging, metrics, or monitoring requires touching pipeline logic
- The `_sse_event()` helper function inside the pipeline file is a code smell

**What I would do instead:**

```
pipeline.py        → Pure execution engine. Returns results via async generator.
                     No knowledge of SSE or HTTP.

transport.py       → Takes pipeline results, formats SSE events, sends to client.
                     Swappable: SSE today, WebSockets tomorrow.

main.py            → Connects the two. Handles HTTP only.
```

**Concrete benefit:**
```python
# Testing pipeline WITHOUT HTTP server becomes possible:
async for step_result in run_pipeline("AI topic"):
    assert step_result["agent"] in EXPECTED_AGENTS
    assert step_result["output"] is not None
```

**The lesson:** Under deadline pressure, mixing concerns is tempting. But separation pays off immediately when you need to test, monitor, or change the transport layer.

---

## 3. Trade-off #1 — Sequential vs Parallel Agent Execution

**What I chose:** Sequential execution (each agent waits for the previous)

**Why this is the only correct choice for the core flow:**
Each agent depends on the previous agent's output as input:
- Analyzer **needs** Retriever's data
- Writer **needs** Analyzer's insights
- Reviewer **needs** Writer's report

True parallelism is impossible for this dependency chain.

**The trade-off:**
| | Sequential (chosen) | Parallel (not possible for core) |
|---|---|---|
| Correctness | ✅ Always correct | ❌ Race conditions |
| Speed | ❌ Slower (~2-3 min) | ✅ Faster |
| Complexity | ✅ Simple to reason about | ❌ Complex synchronization |
| Debuggability | ✅ Easy to trace | ❌ Hard to trace |

**My reasoning:**
Sequential execution with a clear dependency chain is the correct architectural choice here. In production, I would introduce parallelism only at the enrichment level — for example, running a "fetch statistics" and "fetch examples" sub-task simultaneously within the Retriever step using `asyncio.gather()`, while keeping the main agent chain sequential.

---

## 4. Trade-off #2 — Groq Free API vs Paid/Local Model

**What I chose:** Groq API free tier (llama-3.3-70b-versatile)

**Why:**
- Zero infrastructure cost — no GPU, no model serving
- Fast inference — Groq's hardware accelerates LLaMA significantly
- Generous free tier — sufficient for development and demonstration
- Well-documented streaming API — easy to integrate

**The trade-off:**

| | Groq Free API (chosen) | Local Model (e.g. Ollama) | Paid API (Claude/GPT-4) |
|---|---|---|---|
| Cost | ✅ Free | ✅ Free (hardware cost) | ❌ Per-token cost |
| Quality | ✅ Good (70B model) | ⚠️ Depends on hardware | ✅ Best quality |
| Privacy | ❌ Data sent externally | ✅ Fully local | ❌ Data sent externally |
| Rate limits | ❌ Constrained | ✅ None | ⚠️ Paid limits |
| Setup | ✅ Instant | ❌ Complex | ✅ Easy |
| Uptime dependency | ❌ External service | ✅ Always available | ❌ External service |

**My reasoning:**
For a prototype demonstrating system architecture skills, Groq's free API is the right choice — it delivers quality output with zero setup friction. The system is designed so the AI provider is **swappable** — changing from Groq to any other provider requires modifying only `BaseAgent.__init__()`. This loose coupling is intentional and demonstrates sound design thinking.

---

## 5. What I Learned

**Technical:**
- The asyncio.Queue bridge pattern for connecting sync SDKs to async pipelines is essential knowledge for building real streaming systems
- `asyncio.get_running_loop()` vs `get_event_loop()` matters in Python 3.10+ — small details like this show depth of understanding
- SSE is simpler than WebSockets for one-directional streaming and is perfectly suited for this use case

**Architectural:**
- Separating agent concerns (each with its own system prompt and single responsibility) makes the system dramatically easier to debug and improve
- Fallback responses at every step are not optional — they're what makes a system production-ready vs a demo
- The 4th agent (Reviewer) is the differentiator — self-evaluation is a hallmark of mature AI systems

**Process:**
- Building the streaming infrastructure first (BaseAgent + pipeline) was the right call — the agents themselves were easy once the plumbing worked
- Committing incrementally (base agent → individual agents → pipeline → frontend) made the Git history readable and the development process clear