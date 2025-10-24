"""HTTP client for invoking the deployed Lambda LLM proxy."""

from __future__ import annotations

import httpx

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class LLMError(RuntimeError):
    """Raised when the language model interaction fails."""


def _extract_content(payload: object) -> str:
    """Best-effort extraction of generated text from the proxy response."""
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        if "content" in payload and isinstance(payload["content"], str):
            return payload["content"]
        if "response" in payload and isinstance(payload["response"], str):
            return payload["response"]
        if "choices" in payload and payload["choices"]:
            choice = payload["choices"][0]
            if isinstance(choice, dict):
                message = choice.get("message")
                if isinstance(message, dict) and isinstance(message.get("content"), str):
                    return message["content"]
                if isinstance(choice.get("text"), str):
                    return choice["text"]
    raise LLMError("Unexpected response format from LLM proxy.")


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

