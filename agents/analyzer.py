"""
analyzer.py
-----------
AGENT 2 — Analyzer Agent

Job: Take the raw information from the Retriever and extract
meaningful insights, patterns, and conclusions.

This agent transforms raw data → structured insights.
It does NOT write prose — it produces analytical bullets
that the Writer Agent will turn into a report.
"""

from agents.base_agent import BaseAgent


ANALYZER_SYSTEM_PROMPT = """
You are an Expert Analyst Agent — a specialist in extracting deep insights 
from raw research data.

Your job:
1. Identify the 3-5 most significant patterns or trends in the data
2. Draw connections between different data points
3. Highlight what is surprising or counter-intuitive
4. Assess the impact and implications of the findings
5. Rank insights by importance (High / Medium / Low)
6. Identify gaps or areas where more research is needed

Output format:
## Core Insights (ranked by importance)
### Insight 1 [HIGH IMPACT]: <title>
<explanation>

### Insight 2 [HIGH IMPACT]: <title>
<explanation>

... and so on

## Patterns & Connections
[How different facts relate to each other]

## Surprising Findings
[What defies common expectations]

## Impact Assessment
[Real-world implications of these findings]

## Gaps & Limitations
[What we don't know / what's missing]

Be analytical, not descriptive. Focus on WHY things are happening, not just WHAT.
"""


class AnalyzerAgent(BaseAgent):
    """
    Agent 2: Analyzes retrieved data and extracts key insights.
    """

    def __init__(self):
        super().__init__(
            name="Analyzer Agent",
            system_prompt=ANALYZER_SYSTEM_PROMPT,
            max_retries=3
        )

    async def analyze(self, retrieved_data: str, topic: str, stream_callback=None) -> str:
        """
        Analyze the retrieved data and extract insights.

        Args:
            retrieved_data  : Output from the Retriever Agent
            topic           : Original user topic (for context)
            stream_callback : Forward streamed tokens to pipeline

        Returns:
            Structured insights string
        """
        prompt = f"""
        Original Topic: {topic}
        
        Raw Research Data:
        {retrieved_data}
        
        Please analyze this data deeply. Extract the most important insights,
        identify patterns, and assess the real-world implications.
        Focus on what this data actually MEANS, not just what it says.
        """
        return await self.run(prompt, stream_callback)