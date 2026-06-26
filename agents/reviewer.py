"""
reviewer.py
-----------
AGENT 4 — Reviewer Agent  ⭐ (The differentiator!)

Job: Quality-check the generated report. Score it,
flag weak sections, and suggest specific improvements.
Also produces a confidence score that shows up in the UI.

This agent makes the system feel production-grade —
it doesn't just generate, it self-evaluates.
"""

from agents.base_agent import BaseAgent
import re


REVIEWER_SYSTEM_PROMPT = """
You are a Senior Quality Reviewer Agent — a specialist in evaluating 
research reports for accuracy, completeness, clarity, and usefulness.

Your job:
1. Score the report on 5 dimensions (each out of 20, total out of 100)
2. Identify the 2-3 weakest sections with specific reasons
3. Flag any factual gaps or missing perspectives
4. Give specific, actionable improvement suggestions
5. Write an improved Executive Summary if the original is weak

Scoring dimensions:
- Accuracy & Factual Depth (out of 20)
- Clarity & Readability (out of 20)  
- Structure & Organization (out of 20)
- Insight Quality (out of 20)
- Actionability of Recommendations (out of 20)

Output format (follow EXACTLY):

## Quality Review Report

### Overall Score: [XX]/100

### Dimension Scores
| Dimension | Score |
|-----------|-------|
| Accuracy & Factual Depth | [X]/20 |
| Clarity & Readability | [X]/20 |
| Structure & Organization | [X]/20 |
| Insight Quality | [X]/20 |
| Actionability | [X]/20 |

### Strengths
[What the report does well — be specific]

### Weak Sections (Need Improvement)
**Section: [name]** — [specific reason why it's weak]
**Section: [name]** — [specific reason why it's weak]

### Factual Gaps
[What important information is missing]

### Improvement Suggestions
1. [Specific actionable suggestion]
2. [Specific actionable suggestion]  
3. [Specific actionable suggestion]

### Verdict
[APPROVED / NEEDS REVISION] — [one sentence explanation]

Be honest and critical. A mediocre report should get a mediocre score.
"""


class ReviewerAgent(BaseAgent):
    """
    Agent 4: Reviews and scores the generated report.
    Extracts a numeric confidence score for the UI.
    """

    def __init__(self):
        super().__init__(
            name="Reviewer Agent",
            system_prompt=REVIEWER_SYSTEM_PROMPT,
            max_retries=3
        )

    async def review(self, report: str, topic: str, stream_callback=None) -> dict:
        """
        Review the generated report and return review + score.

        Args:
            report          : The report written by the Writer Agent
            topic           : Original user topic
            stream_callback : Forward streamed tokens to pipeline

        Returns:
            dict with keys:
              - 'review_text' : Full review markdown
              - 'score'       : Integer score out of 100
              - 'verdict'     : 'APPROVED' or 'NEEDS REVISION'
        """
        prompt = f"""
        Topic: {topic}
        
        === REPORT TO REVIEW ===
        {report}
        
        Please review this report critically and thoroughly.
        Score it honestly. Flag every weakness you find.
        """

        review_text = await self.run(prompt, stream_callback)

        # ── Extract numeric score from the review text ──────────────────
        score = self._extract_score(review_text)
        verdict = "APPROVED" if score >= 70 else "NEEDS REVISION"

        return {
            "review_text": review_text,
            "score": score,
            "verdict": verdict
        }

    def _extract_score(self, review_text: str) -> int:
        """
        Parse the Overall Score from the review text using regex.
        Falls back to 70 if parsing fails.
        """
        # Look for pattern like "Overall Score: 84/100" or "Score: 84/100"
        match = re.search(r"Overall Score[:\s]+(\d+)/100", review_text, re.IGNORECASE)
        if match:
            score = int(match.group(1))
            return max(0, min(100, score))  # clamp between 0–100

        # Fallback: look for any XX/100 pattern
        match = re.search(r"(\d{2,3})/100", review_text)
        if match:
            return max(0, min(100, int(match.group(1))))

        return 70  # safe default if parsing fails