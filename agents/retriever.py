"""
retriever.py
------------
AGENT 1 — Retriever Agent

Job: Given a topic, gather and organize all relevant background
information, facts, statistics, and context needed for analysis.

In a production system this agent would call real search APIs
(Tavily, SerpAPI, etc.). Here it uses Claude's knowledge base
to simulate retrieval — making it easy to swap in a real API later.
"""

from agents.base_agent import BaseAgent


# ── System prompt defines this agent's entire personality ──────────────────
RETRIEVER_SYSTEM_PROMPT = """
You are a Research Retriever Agent — a specialist in gathering comprehensive, 
factual information on any given topic.

Your job:
1. Identify the 5-7 most important facts, statistics, and data points about the topic
2. Find recent trends (last 2-3 years) related to the topic
3. Identify key players, organizations, or figures involved
4. Note any controversies or debates around the topic
5. Organize everything in a clear, structured format for the next agent

Output format:
## Key Facts & Statistics
[bullet points]

## Recent Trends
[bullet points]

## Key Players / Organizations
[bullet points]

## Controversies / Debates
[bullet points]

## Raw Data Summary
[2-3 paragraph summary of everything gathered]

Be factual, specific, and comprehensive. Avoid opinions — just gather information.
"""


class RetrieverAgent(BaseAgent):
    """
    Agent 1: Retrieves and structures raw information about the topic.
    """

    def __init__(self):
        super().__init__(
            name="Retriever Agent",
            system_prompt=RETRIEVER_SYSTEM_PROMPT,
            max_retries=3
        )

    async def retrieve(self, topic: str, stream_callback=None) -> str:
        """
        Gather information about the given topic.

        Args:
            topic           : The research topic from the user
            stream_callback : Forward streamed tokens to pipeline

        Returns:
            Structured information string
        """
        prompt = f"""
        Please research the following topic thoroughly:
        
        TOPIC: {topic}
        
        Gather all relevant facts, statistics, trends, and context.
        Structure your output clearly for downstream analysis.
        """
        return await self.run(prompt, stream_callback)