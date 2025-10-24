"""Agent service orchestrating planning, retrieval, LLM generation, and execution."""

from __future__ import annotations

from dataclasses import dataclass

from app.agent.llm import LambdaLLMClient
from app.agent.planner import Intent, plan_intent
from app.agent.prompts import build_sql_prompt
from app.agent.retriever import ChromaRetriever, RetrievalError
from app.agent.sql_executor import execute_query
from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class AgentResponse:
    sql: str
    rows: list[dict]
    columns: list[str]
    intent: Intent
    limit_enforced: bool


class AgentService:
    """Main entrypoint for serving natural-language data questions."""

    def __init__(self) -> None:
        self._llm = LambdaLLMClient()
        self._retriever = ChromaRetriever()

    def handle_query(self, prompt: str) -> AgentResponse:
        plan = plan_intent(prompt)
        if plan.intent is not Intent.SQL:
            raise NotImplementedError(f"Intent {plan.intent.name} not yet supported")

        try:
            context = self._retriever.retrieve(prompt)
        except RetrievalError as exc:
            logger.exception("Retriever failed")
            raise

        limit = get_settings().default_result_limit
        sql_prompt = build_sql_prompt(prompt, context, limit=limit)
        sql = self._llm.generate(sql_prompt)

        result = execute_query(sql)
        return AgentResponse(
            sql=result["sql"],
            rows=result["rows"],
            columns=list(result["columns"]),
            intent=plan.intent,
            limit_enforced=result["limit_enforced"],
        )

