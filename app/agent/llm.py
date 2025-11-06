"""HTTP client for invoking the deployed Lambda LLM proxy."""

from __future__ import annotations

import json
import re

import httpx

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class LLMError(RuntimeError):
    """Raised when the language model interaction fails."""


def _strip_code_fence(text: str) -> str:
    """Remove common Markdown code fences from LLM responses."""
    pattern = re.compile(r"^```(?:\w+)?\s*(.*?)\s*```$", re.DOTALL)
    match = pattern.match(text.strip())
    if match:
        return match.group(1).strip()
    return text


def _extract_content(payload: object) -> str:
    """Best-effort extraction of generated text from the proxy response."""
    if isinstance(payload, str):
        return _strip_code_fence(payload)
    if isinstance(payload, dict):
        # Unwrap common proxy envelope formats
        body = payload.get("body")
        if isinstance(body, str):
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                return _strip_code_fence(body)
        elif isinstance(body, dict):
            payload = body

        reply = payload.get("reply")
        if isinstance(reply, str):
            return _strip_code_fence(reply)

        sql = payload.get("sql")
        if isinstance(sql, str):
            return _strip_code_fence(sql)

        content = payload.get("content")
        if isinstance(content, str):
            return _strip_code_fence(content)

        response = payload.get("response")
        if isinstance(response, str):
            return _strip_code_fence(response)

        choices = payload.get("choices")
        if choices:
            choice = choices[0]
            if isinstance(choice, dict):
                message = choice.get("message")
                if isinstance(message, dict):
                    message_content = message.get("content")
                    if isinstance(message_content, str):
                        return _strip_code_fence(message_content)
                choice_text = choice.get("text")
                if isinstance(choice_text, str):
                    return _strip_code_fence(choice_text)
    raise LLMError(f"Unexpected response format from LLM proxy: {payload!r}")


class LambdaLLMClient:
    """Thin wrapper around the Lambda HTTP endpoint that fronts the OpenAI call."""

    def __init__(self) -> None:
        settings = get_settings()
        self._url = settings.llm_proxy_url
        self._client = httpx.Client(timeout=30.0)

    def generate(self, prompt: str) -> str:
        """Send prompt to the proxy and return generated SQL."""
        try:
            response = self._client.post(
                self._url,
                json={"prompt": prompt},
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - passthrough network errors
            raise LLMError(f"LLM proxy request failed: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise LLMError("LLM proxy returned invalid JSON.") from exc

        content = _extract_content(payload).strip()
        logger.debug("LLM proxy response: %s", content)
        return content

    def close(self) -> None:
        self._client.close()

    def __del__(self) -> None:  # pragma: no cover - best effort cleanup
        try:
            self.close()
        except Exception:  # noqa: broad-except
            pass

