"""Helpers for persisting per-session conversation state in Redis."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.core.cache import append_json_list, get_json, get_json_list, set_json

CHAT_TTL_SECONDS = 60 * 60 * 6  # 6 hours
MAX_TURNS_STORED = 40


def _history_key(session_id: str) -> str:
    return f"chat:{session_id}:history"


def _meta_key(session_id: str) -> str:
    return f"chat:{session_id}:meta"


def append_user_turn(session_id: str, prompt: str) -> None:
    turn = {
        "role": "user",
        "prompt": prompt,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }
    append_json_list(
        _history_key(session_id),
        turn,
        ttl=CHAT_TTL_SECONDS,
        max_items=MAX_TURNS_STORED,
    )


def append_agent_turn(session_id: str, turn: Dict[str, Any]) -> None:
    payload = {
        **turn,
        "role": "agent",
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }
    append_json_list(
        _history_key(session_id),
        payload,
        ttl=CHAT_TTL_SECONDS,
        max_items=MAX_TURNS_STORED,
    )


def get_history(session_id: str) -> List[Dict[str, Any]]:
    return get_json_list(_history_key(session_id))


def get_last_intent(session_id: str) -> Optional[str]:
    meta = get_json(_meta_key(session_id))
    if isinstance(meta, dict):
        intent = meta.get("last_intent")
        if isinstance(intent, str):
            return intent
    return None


def set_last_intent(session_id: str, intent: str) -> None:
    payload = {
        "last_intent": intent,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    set_json(_meta_key(session_id), payload, ttl=CHAT_TTL_SECONDS)


