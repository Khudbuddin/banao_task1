"""
base_agent.py
-------------
The foundation class for all agents in the pipeline.
Every specialized agent (Retriever, Analyzer, Writer, Reviewer)
inherits from this class.

Key responsibilities:
- Hold the agent's identity (name, system prompt)
- Call Groq API directly (no frameworks)
- Handle TRUE token-by-token streaming via asyncio.Queue
- Retry on failure with exponential backoff

Provider: Groq (free tier) — using Llama 3.3 70B model

HOW REAL STREAMING WORKS HERE:
  The Groq SDK is synchronous, so we run it in a ThreadPoolExecutor.
  The sync thread pushes each token into an asyncio.Queue as it arrives.
  The async run() method reads tokens from the queue in real time
  and fires stream_callback(token) for each one — so the frontend
  sees tokens arrive one by one, not all at once at the end.

  Sentinel value None signals the queue that the stream is done.
"""

import os
import asyncio
from groq import Groq
from dotenv import load_dotenv

load_dotenv()


class BaseAgent:
    """
    Base class for all pipeline agents.
    Wraps the Groq API directly — no black-box frameworks.
    """

    def __init__(self, name: str, system_prompt: str, max_retries: int = 3):
        """
        Args:
            name         : Human-readable agent name (e.g. 'Retriever Agent')
            system_prompt: Defines this agent's personality and job
            max_retries  : How many times to retry on API failure
        """
        self.name = name
        self.system_prompt = system_prompt
        self.max_retries = max_retries

        # Direct Groq API client — we control every parameter
        self.client = Groq(
            api_key=os.getenv("GROQ_API_KEY")
        )

        # Model — llama-3.3-70b is Groq's best free model
        self.model = "llama-3.3-70b-versatile"

    async def run(self, user_input: str, stream_callback=None) -> str:
        """
        Send input to Groq and return the full response.
        Streams tokens live via stream_callback — TRUE token-by-token.

        Args:
            user_input      : The prompt/context passed to this agent
            stream_callback : async function(token: str) — called for EACH token
                              as it arrives from the API, not after completion.

        Returns:
            Full response text as a string (all tokens joined)
        """
        attempt = 0

        while attempt < self.max_retries:
            try:
                # ── asyncio.Queue bridges the sync Groq stream → async world ──
                # The sync thread puts tokens here; this coroutine reads them.
                token_queue: asyncio.Queue = asyncio.Queue()
                # get_running_loop() is correct in Python 3.10+ (vs deprecated get_event_loop)
                loop = asyncio.get_running_loop()

                def _sync_stream_to_queue():
                    """
                    Runs in a ThreadPoolExecutor (non-blocking for async loop).
                    Opens the Groq stream and pushes each token into the queue
                    as it arrives — real-time, not buffered.
                    Puts None when done as a sentinel to signal end-of-stream.
                    """
                    try:
                        stream = self.client.chat.completions.create(
                            model=self.model,
                            max_tokens=1500,
                            messages=[
                                {"role": "system", "content": self.system_prompt},
                                {"role": "user",   "content": user_input}
                            ],
                            stream=True
                        )
                        for chunk in stream:
                            token = chunk.choices[0].delta.content or ""
                            if token:
                                # thread-safe: put_nowait via call_soon_threadsafe
                                loop.call_soon_threadsafe(token_queue.put_nowait, token)

                    except Exception as e:
                        # Push the exception into the queue so async side can raise it
                        loop.call_soon_threadsafe(token_queue.put_nowait, e)
                    finally:
                        # Always send sentinel so the consumer loop exits
                        loop.call_soon_threadsafe(token_queue.put_nowait, None)

                # Start the sync streaming in a background thread
                loop.run_in_executor(None, _sync_stream_to_queue)

                # ── Consume tokens from the queue as they arrive ───────────
                collected_tokens = []

                while True:
                    token = await token_queue.get()

                    # Sentinel received — stream is complete
                    if token is None:
                        break

                    # If the thread pushed an exception, re-raise it here
                    if isinstance(token, Exception):
                        raise token

                    collected_tokens.append(token)

                    # Fire callback with this single token — real-time streaming
                    if stream_callback:
                        await stream_callback(token)

                full_response = "".join(collected_tokens)
                return full_response

            except Exception as e:
                error_msg = str(e).lower()

                # Rate limit — wait longer and retry
                if "rate limit" in error_msg or "429" in error_msg:
                    attempt += 1
                    wait_time = 5 * attempt
                    print(f"[{self.name}] Rate limited. Waiting {wait_time}s... "
                          f"(attempt {attempt}/{self.max_retries})")
                    await asyncio.sleep(wait_time)

                # Connection/timeout — exponential backoff
                elif "connection" in error_msg or "timeout" in error_msg:
                    attempt += 1
                    wait_time = 2 ** attempt  # 2s, 4s, 8s
                    print(f"[{self.name}] Connection error. Retrying in {wait_time}s... "
                          f"(attempt {attempt}/{self.max_retries})")
                    await asyncio.sleep(wait_time)

                # Auth error — never retry, surface immediately
                elif "auth" in error_msg or "401" in error_msg or "api key" in error_msg:
                    print(f"[{self.name}] Authentication error — check GROQ_API_KEY in .env")
                    raise

                # Unknown error — retry once with short wait
                else:
                    attempt += 1
                    print(f"[{self.name}] Unexpected error: {e}. "
                          f"Retrying... (attempt {attempt}/{self.max_retries})")
                    await asyncio.sleep(2)

        # All retries exhausted — raise so pipeline can use fallback
        raise RuntimeError(
            f"[{self.name}] Failed after {self.max_retries} attempts. "
            "Check your GROQ_API_KEY and network connection."
        )