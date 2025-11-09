"""Lightweight LLM-based intent classifier for routing user prompts."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from app.agent.llm import LambdaLLMClient, LLMError
from app.agent.planner import Intent

INTENT_CHOICES = {"SQL", "ETL", "FOLLOWUP"}


class IntentClassifier:
    """Uses a lightweight LLM call to classify the user's intent."""

    def __init__(self) -> None:
        self._llm = LambdaLLMClient()

    def classify(
        self,
        prompt: str,
        history: List[Dict[str, str]],
    ) -> Tuple[Optional[Intent], bool]:
        """Return (intent, reuse_last_flag)."""

        history_lines = []
        for turn in history[-4:]:
            role = turn.get("role")
            if role == "user":
                history_lines.append(f"User: {turn.get('prompt', '')}")
            elif role == "agent":
                intent = turn.get("intent", "")
                summary = turn.get("summary") or turn.get("sql") or ""
                if summary and len(summary) > 400:
                    summary = summary[:400] + " ..."
                history_lines.append(f"Agent ({intent}): {summary}")

        context_block = "\n".join(history_lines)
        instruction = (
            "You are an intent classifier for a data analytics assistant. "
            "Given the recent conversation, decide whether the user's next request "
            "requires running a SQL query, executing an ETL pipeline, or simply follows up "
            "on the previous response without changing the intent.\n\n"
            "Output exactly one token from the set: SQL, ETL, FOLLOWUP."
        )
        prompt_block = (
            f"{instruction}\n\n"
            f"Conversation history:\n{context_block or '(none)'}\n\n"
            f"User request: {prompt}\n\n"
            "Answer:"
        )

        try:
            label = self._llm.generate(prompt_block).strip().upper()
        except LLMError:
            return None, False

        if label not in INTENT_CHOICES:
            return None, False

        if label == "FOLLOWUP":
            return None, True

        return Intent[label], False


