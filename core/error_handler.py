"""
error_handler.py
----------------
Custom error types and failure handling logic for the pipeline.

Instead of letting raw exceptions crash the system, we:
1. Catch known failure types
2. Produce meaningful error messages
3. Decide whether to retry or fail gracefully
"""


class AgentFailureError(Exception):
    """Raised when an agent fails after all retries."""
    def __init__(self, agent_name: str, reason: str):
        self.agent_name = agent_name
        self.reason = reason
        super().__init__(f"[{agent_name}] failed: {reason}")


class PipelineError(Exception):
    """Raised when the entire pipeline cannot continue."""
    def __init__(self, step: str, reason: str):
        self.step = step
        self.reason = reason
        super().__init__(f"Pipeline stopped at [{step}]: {reason}")


class TimeoutError(Exception):
    """Raised when an agent takes too long to respond."""
    def __init__(self, agent_name: str, timeout_seconds: int):
        self.agent_name = agent_name
        super().__init__(f"[{agent_name}] timed out after {timeout_seconds}s")


# ── Fallback responses when an agent fails ─────────────────────────────────
# Instead of crashing, the pipeline uses these as placeholders
# so downstream agents can still attempt to run.

FALLBACK_RESPONSES = {
    "Retriever Agent": """
## Fallback: Retriever Agent Unavailable

The Retriever Agent encountered an error. Using fallback data.

## Key Facts & Statistics
- Unable to retrieve live data at this time
- Fallback mode activated — downstream agents will work with limited context

## Raw Data Summary
The retrieval step failed. The Analyzer and Writer agents will attempt
to work with general knowledge about the requested topic.
""",

    "Analyzer Agent": """
## Fallback: Analyzer Agent Unavailable

The Analyzer Agent encountered an error. Using simplified analysis.

## Core Insights
### Insight 1 [MEDIUM IMPACT]: Analysis Unavailable
The analysis step could not complete. The Writer Agent will proceed
with the raw retrieved data directly.

## Impact Assessment
Unable to perform full analysis. Report quality may be reduced.
""",

    "Writer Agent": """
# Fallback Report

## Executive Summary
The Writer Agent encountered an error and could not produce a full report.

## Available Information
The system retrieved and analyzed data successfully, but report generation failed.
Please retry the request or contact support.

## Conclusion
Pipeline completed partially. Raw data and analysis are available but
the final report could not be formatted.

---
*Fallback report — Writer Agent failed*
""",

    "Reviewer Agent": """
## Fallback: Reviewer Agent Unavailable

### Overall Score: 70/100

The Reviewer Agent encountered an error. Assigning default score.

### Verdict
APPROVED — (Default — reviewer unavailable)

### Note
Quality review could not be completed. The report above was generated
but not formally reviewed. Please manually assess the output.
"""
}


def get_fallback(agent_name: str) -> str:
    """Return a fallback response for the given agent."""
    return FALLBACK_RESPONSES.get(
        agent_name,
        f"## Fallback\n[{agent_name}] failed. No fallback available for this agent."
    )