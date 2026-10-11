from __future__ import annotations

import re
from dataclasses import dataclass


COMMAND_ALIASES = {
    "!도움말": "help",
    "!help": "help",
    "!통계": "stats",
    "!stats": "stats",
    "!통계 대화방": "stats group",
    "!stats group": "stats group",
    "!통계 나": "stats me",
    "!stats me": "stats me",
    "!통계 전체": "stats all",
    "!stats all": "stats all",
    "!멘션": "mentions",
    "!mentions": "mentions",
    "!데이터베이스": "db",
    "!db": "db",
    "!경고": "warnings",
    "!경고주의": "warning_alerts",
    "!소통미달": "low_activity",
    "!소통설정": "activity_settings",
    "!키워드": "keyword_settings",
    "!입퇴장": "membership",
}


@dataclass(frozen=True, slots=True)
class ParsedCommand:
    name: str | None
    stats_day_offset: int = 0
    low_activity_offset: int = 0
    activity_settings_argument: str = ""
    keyword_settings_argument: str = ""


def parse_command(text: str) -> ParsedCommand:
    """Normalize a message into a command while preserving legacy syntax."""
    command = COMMAND_ALIASES.get(text.casefold())

    stats_match = re.fullmatch(r"!통계\s+-(\d+)", text)
    stats_day_offset = int(stats_match.group(1)) if stats_match else 0
    if stats_match:
        command = "stats day"
    elif re.match(r"^!경고(?:\s|$)", text):
        command = "warnings"

    low_activity_match = re.fullmatch(r"!소통미달(?:\s+-(\d+))?", text)
    low_activity_offset = (
        int(low_activity_match.group(1) or 0) if low_activity_match else 0
    )
    if low_activity_match:
        command = "low_activity"

    if re.fullmatch(r"!입퇴장(?:\s+(.+))?", text):
        command = "membership"

    activity_settings_match = re.fullmatch(r"!소통설정(?:\s+(.+))?", text)
    activity_settings_argument = (
        activity_settings_match.group(1).strip()
        if activity_settings_match and activity_settings_match.group(1)
        else ""
    )
    if activity_settings_match:
        command = "activity_settings"

    keyword_settings_match = re.fullmatch(r"!키워드(?:\s+(.+))?", text)
    keyword_settings_argument = (
        keyword_settings_match.group(1).strip()
        if keyword_settings_match and keyword_settings_match.group(1)
        else ""
    )
    if keyword_settings_match:
        command = "keyword_settings"

    return ParsedCommand(
        name=command,
        stats_day_offset=stats_day_offset,
        low_activity_offset=low_activity_offset,
        activity_settings_argument=activity_settings_argument,
        keyword_settings_argument=keyword_settings_argument,
    )
