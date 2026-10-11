from __future__ import annotations

from datetime import datetime, timezone


def field(obj, *names, default=None):
    """Read the first non-None field from a dict or SDK model."""
    for name in names:
        value = obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)
        if value is not None:
            return value
    return default


def utc_now() -> datetime:
    """현재 UTC 시각을 timezone-aware datetime으로 반환한다."""
    return datetime.now(timezone.utc)


def iso_now() -> str:
    """현재 UTC 시각을 ISO 8601 문자열로 반환한다."""
    return utc_now().isoformat()


def source_identity(source) -> tuple[str | None, str | None, str]:
    """Return (user_id, chat_id, source_type) without inventing missing IDs."""
    source_type = field(source, "type", default="unknown")
    user_id = field(source, "user_id", "userId")
    if source_type == "group":
        raw = field(source, "group_id", "groupId")
        return user_id, (f"group:{raw}" if raw else None), source_type
    if source_type == "room":
        raw = field(source, "room_id", "roomId")
        return user_id, (f"room:{raw}" if raw else None), source_type
    if source_type == "user":
        return user_id, (f"user:{user_id}" if user_id else None), source_type
    return user_id, None, source_type


def mention_targets(message) -> tuple[list[str], int, bool]:
    """Parse individual and all-user mentions from LINE metadata."""
    mention = field(message, "mention")
    mentionees = field(mention, "mentionees", default=[]) or []
    targets: list[str] = []
    unresolved = 0
    all_mentioned = False

    for person in mentionees:
        kind = str(field(person, "type", default="") or "").lower()
        if kind in {"all", "everyone"}:
            all_mentioned = True
            continue
        if kind != "user":
            unresolved += 1
            continue

        user_id = field(person, "user_id", "userId")
        if user_id:
            targets.append(str(user_id))
        else:
            unresolved += 1

    return list(dict.fromkeys(targets)), unresolved, all_mentioned


def event_occurred_at(event) -> str:
    """Convert LINE's millisecond timestamp into an ISO UTC string."""
    timestamp = field(event, "timestamp")
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp / 1000, timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()
