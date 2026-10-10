from __future__ import annotations

import asyncio
import logging
import os
import re
import json
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    AsyncApiClient,
    AsyncMessagingApi,
    Configuration,
    ReplyMessageRequest,
    PushMessageRequest,
    TextMessage,
)
from linebot.v3.webhook import WebhookParser
from linebot.v3.webhooks import MessageEvent

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_POOL_MIN = max(1, int(os.getenv("DB_POOL_MIN", "1")))
DB_POOL_MAX = max(DB_POOL_MIN, int(os.getenv("DB_POOL_MAX", "5")))
db_pool: asyncpg.Pool | None = None
CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
ADMIN_USER_IDS = {x.strip() for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()}
ECHO_MESSAGES = os.getenv("ECHO_MESSAGES", "false").lower() in {"1", "true", "yes"}
TIMEZONE_NAME = os.getenv("TIMEZONE", "Asia/Seoul")
RETENTION_DAYS = max(0, int(os.getenv("RETENTION_DAYS", "180")))
MENTION_BATCH_LIMIT = max(1, min(20, int(os.getenv("MENTION_BATCH_LIMIT", "5"))))
# 이모티콘은 LINE 프로필 표시 이름에 포함된 문자열과 정확히 일치하는 항목으로 분류한다.
def _parse_emoji_group(env_name: str, default: str) -> tuple[str, ...]:
    values = tuple(dict.fromkeys(item.strip() for item in os.getenv(env_name, default).split(",") if item.strip()))
    return values


MALE_GROUP_EMOJIS = _parse_emoji_group("MALE_GROUP_EMOJIS", "🔭,💫")
FEMALE_GROUP_EMOJIS = _parse_emoji_group("FEMALE_GROUP_EMOJIS", "📡")
MALE_DAILY_MIN_MESSAGES = max(1, int(os.getenv("MALE_DAILY_MIN_MESSAGES", "80")))
FEMALE_DAILY_MIN_MESSAGES = max(1, int(os.getenv("FEMALE_DAILY_MIN_MESSAGES", "60")))
if set(MALE_GROUP_EMOJIS) & set(FEMALE_GROUP_EMOJIS):
    overlap = ", ".join(sorted(set(MALE_GROUP_EMOJIS) & set(FEMALE_GROUP_EMOJIS)))
    raise RuntimeError(f"남성/여성 이모티콘 설정이 중복됩니다: {overlap}. 환경변수를 확인해 주세요.")
WARNING_ALERT_THRESHOLD = max(1, int(os.getenv("WARNING_ALERT_THRESHOLD", "3")))
MAX_REPLY_CHARS = 4500
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

# 한국어 명령어를 기본으로 사용하며, 기존 영문 명령어도 호환성을 위해 지원한다.
COMMAND_ALIASES = {
    "!도움말": "help", "!help": "help",
    "!통계": "stats", "!stats": "stats",
    "!통계 대화방": "stats group", "!stats group": "stats group",
    "!통계 나": "stats me", "!stats me": "stats me",
    "!통계 전체": "stats all", "!stats all": "stats all",
    "!멘션": "mentions", "!mentions": "mentions",
    "!데이터베이스": "db", "!db": "db",
    "!경고": "warnings",
    "!경고주의": "warning_alerts",
    "!소통미달": "low_activity",
    "!소통설정": "activity_settings",
    "!입퇴장": "membership",
}
EXPORT_TTL = max(60, int(os.getenv("EXPORT_TTL", "600")))
EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "/tmp/line_bot_exports"))
export_tokens: dict[str, tuple[str, float]] = {}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("linebot")

if not CHANNEL_SECRET or not CHANNEL_ACCESS_TOKEN:
    raise RuntimeError(".env 파일에 LINE_CHANNEL_SECRET과 LINE_CHANNEL_ACCESS_TOKEN을 설정해야 합니다.")

configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)



def asyncpg_connection_args(dsn: str) -> tuple[str, str | None]:
    """Normalize Neon/libpq URLs for asyncpg and require TLS for remote DBs."""
    parts = urlsplit(dsn)
    local = parts.hostname in {"localhost", "127.0.0.1", "::1"}
    # SSL is passed as an asyncpg option below; remove libpq-only URL switches
    # to avoid duplicate/unsupported keyword arguments in some asyncpg versions.
    query = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True)
             if key.lower() not in {"sslmode", "channel_binding"}]
    normalized = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
    return normalized, (None if local else "require")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def format_datetime(value: str | None) -> str:
    """ISO 시각을 한국어 날짜·시간 형식으로 표시한다."""
    if not value:
        return "기록 없음"
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo(TIMEZONE_NAME)).strftime("%Y년 %m월 %d일 %H:%M")
    except (ValueError, TypeError):
        return str(value)


def classify_user_group(display_name: str | None) -> tuple[str, int] | None:
    """등록된 남성/여성 이모티콘이 명확히 일치할 때만 그룹과 기준을 반환한다.

    어느 그룹에도 속하지 않거나 두 그룹 이모티콘이 모두 있으면 소통량 판정에서 제외한다.
    """
    name = display_name or ""
    male_match = any(emoji in name for emoji in MALE_GROUP_EMOJIS)
    female_match = any(emoji in name for emoji in FEMALE_GROUP_EMOJIS)
    if male_match and not female_match:
        return "남성", MALE_DAILY_MIN_MESSAGES
    if female_match and not male_match:
        return "여성", FEMALE_DAILY_MIN_MESSAGES
    return None


def minimum_messages_for_name(display_name: str | None) -> int | None:
    classification = classify_user_group(display_name)
    return classification[1] if classification else None


def cleanup_expired_exports() -> None:
    now = time.time()
    for token, (file_path, expires_at) in list(export_tokens.items()):
        if expires_at <= now:
            export_tokens.pop(token, None)
            try:
                Path(file_path).unlink(missing_ok=True)
            except OSError:
                log.warning("만료된 내보내기 파일 삭제 실패: %s", file_path)
    if EXPORT_DIR.exists():
        for path in EXPORT_DIR.glob("database_*.json"):
            try:
                if now - path.stat().st_mtime > EXPORT_TTL * 2:
                    path.unlink(missing_ok=True)
            except OSError:
                log.warning("오래된 내보내기 파일 정리 실패: %s", path)


async def export_database_to_json(output_path: Path) -> None:
    """PostgreSQL의 공개 테이블을 JSON으로 내보낸다."""
    if db_pool is None:
        raise RuntimeError("데이터베이스 풀이 아직 초기화되지 않았습니다.")
    async with db_pool.acquire() as conn:
        table_rows = await conn.fetch("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename NOT LIKE 'pg_%'
            ORDER BY tablename
        """)
        result = {
            "exported_at": datetime.now(ZoneInfo(TIMEZONE_NAME)).isoformat(),
            "database": "PostgreSQL",
            "tables": {},
        }
        for item in table_rows:
            table_name = item["tablename"]
            # 테이블명은 PostgreSQL 시스템 카탈로그에서 얻은 값만 사용한다.
            safe_name = table_name.replace('"', '""')
            rows = await conn.fetch(f'SELECT * FROM "{safe_name}"')
            result["tables"][table_name] = [dict(row) for row in rows]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=False, indent=2, default=str)


async def create_database_export() -> str:
    cleanup_expired_exports()
    if not PUBLIC_BASE_URL:
        raise RuntimeError("PUBLIC_BASE_URL 환경변수가 설정되지 않았습니다.")
    token = secrets.token_urlsafe(32)
    file_path = EXPORT_DIR / f"database_{token}.json"
    try:
        await export_database_to_json(file_path)
        export_tokens[token] = (str(file_path), time.time() + EXPORT_TTL)
        return f"{PUBLIC_BASE_URL}/exports/{token}"
    except Exception:
        file_path.unlink(missing_ok=True)
        raise


def field(obj, *names, default=None):
    for name in names:
        if isinstance(obj, dict):
            value = obj.get(name)
        else:
            value = getattr(obj, name, None)
        if value is not None:
            return value
    return default


def source_identity(source) -> tuple[str | None, str | None, str]:
    """Return (user_id, chat_id, source_type). Never invent missing IDs."""
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
    """LINE 멘션 메타데이터에서 개인 멘션과 전체 멘션을 구분한다."""
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
        uid = field(person, "user_id", "userId")
        if uid:
            targets.append(str(uid))
        else:
            unresolved += 1
    return list(dict.fromkeys(targets)), unresolved, all_mentioned


SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  message_count INTEGER NOT NULL DEFAULT 0,
  display_name TEXT,
  display_name_updated_at TEXT
);
CREATE TABLE IF NOT EXISTS messages (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  event_id TEXT UNIQUE,
  line_message_id TEXT,
  user_id TEXT,
  chat_id TEXT NOT NULL,
  message_type TEXT NOT NULL,
  message_text TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_user_time ON messages(user_id, created_at);
CREATE TABLE IF NOT EXISTS mentions (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  event_id TEXT,
  chat_id TEXT NOT NULL,
  sender_id TEXT NOT NULL,
  target_id TEXT,
  notification_type TEXT NOT NULL DEFAULT 'mention'
    CHECK(notification_type IN ('mention','reply','all_mention','unresolved')),
  original_text TEXT NOT NULL,
  created_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK(status IN ('pending','processing','delivered','unresolved')),
  processing_token TEXT,
  processing_since TEXT,
  delivered_at TEXT
);
CREATE TABLE IF NOT EXISTS moderation_warnings (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  chat_id TEXT NOT NULL,
  target_id TEXT NOT NULL,
  issuer_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  cancelled_by TEXT,
  cancelled_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_warnings_chat_target_active ON moderation_warnings(chat_id, target_id, active, id);
CREATE TABLE IF NOT EXISTS processed_events (
  event_id TEXT PRIMARY KEY,
  processed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bot_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS membership_events (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  event_id TEXT,
  chat_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK(event_type IN ('joined','left')),
  occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_membership_chat_time ON membership_events(chat_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_membership_user_time ON membership_events(chat_id, user_id, occurred_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_membership_event_dedupe
  ON membership_events(event_id, user_id, event_type) WHERE event_id IS NOT NULL;
"""


class PGCursor:
    """기존 DB 호출부와 호환되는 최소 커서 인터페이스를 제공한다."""
    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return self._rows


class PGConnection:
    def __init__(self, conn):
        self.conn = conn

    @staticmethod
    def _sql(sql: str) -> str:
        # 이 프로젝트의 SQL에는 물음표가 매개변수 자리표시자로만 사용된다.
        index = 0
        def replace(_match):
            nonlocal index
            index += 1
            return f"${index}"
        sql = re.sub(r"\?", replace, sql)
        sql = re.sub(r"INSERT OR IGNORE INTO", "INSERT INTO", sql, flags=re.IGNORECASE)
        if re.search(r"INSERT INTO processed_events\s*\(", sql, flags=re.IGNORECASE) and "ON CONFLICT" not in sql.upper():
            sql = sql.rstrip().rstrip(";") + " ON CONFLICT (event_id) DO NOTHING"
        return sql

    async def execute(self, sql, params=()):
        """Execute SQL while preserving the small cursor API used by this app."""
        sql = self._sql(sql.strip())
        upper = sql.lstrip().upper()
        bound_params = tuple(params) if params else ()

        # Fetching rows directly supports SELECT and DML with RETURNING clauses.
        if upper.startswith(("SELECT", "WITH")) or " RETURNING " in upper:
            rows = await self.conn.fetch(sql, *bound_params)
            return PGCursor(rows, len(rows))

        # asyncpg status strings look like "INSERT 0 1", "UPDATE 2", etc.
        status = await self.conn.execute(sql, *bound_params)
        match = re.fullmatch(
            r"(?:INSERT\s+\d+\s+(\d+)|UPDATE\s+(\d+)|DELETE\s+(\d+)|SELECT\s+(\d+))",
            status.strip(),
            flags=re.IGNORECASE,
        )
        rowcount = int(next(value for value in match.groups() if value is not None)) if match else 0
        return PGCursor([], rowcount)

    async def commit(self):
        # Raw BEGIN transactions are committed through SQL in asyncpg.
        await self.conn.execute("COMMIT")

    async def rollback(self):
        await self.conn.execute("ROLLBACK")


@asynccontextmanager
async def db_connect():
    if db_pool is None:
        raise RuntimeError("DATABASE_URL 연결 풀이 초기화되지 않았습니다.")
    async with db_pool.acquire() as conn:
        yield PGConnection(conn)


async def init_db():
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL 환경변수를 설정해 주세요. Neon PostgreSQL 연결 문자열을 사용해야 합니다."
        )
    normalized_dsn, ssl_setting = asyncpg_connection_args(DATABASE_URL)
    db_pool = await asyncpg.create_pool(
        dsn=normalized_dsn,
        min_size=DB_POOL_MIN,
        max_size=DB_POOL_MAX,
        command_timeout=30,
        ssl=ssl_setting,
    )
    try:
        async with db_pool.acquire() as conn:
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    await conn.execute(statement)
            # 기존 PostgreSQL 스키마가 일부 컬럼 없이 만들어졌더라도 안전하게 추가한다.
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name_updated_at TEXT")
            await conn.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS line_message_id TEXT")
            await conn.execute("ALTER TABLE mentions ADD COLUMN IF NOT EXISTS notification_type TEXT NOT NULL DEFAULT 'mention'")
            await conn.execute("ALTER TABLE mentions ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending'")
            await conn.execute("ALTER TABLE mentions ADD COLUMN IF NOT EXISTS processing_token TEXT")
            await conn.execute("ALTER TABLE mentions ADD COLUMN IF NOT EXISTS processing_since TEXT")
            await conn.execute("ALTER TABLE mentions ADD COLUMN IF NOT EXISTS delivered_at TEXT")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mentions_pending ON mentions(chat_id, target_id, status, id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_line_id ON messages(chat_id, line_message_id)")
            # 이전 스키마에 status가 없었다면 기존 delivered 값이 있던 행을 최대한 보존한다.
            old_columns = await conn.fetch("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='public' AND table_name='mentions'
            """)
            names = {row["column_name"] for row in old_columns}
            if "delivered" in names:
                await conn.execute("""
                    UPDATE mentions SET status = CASE WHEN delivered THEN 'delivered' ELSE 'pending' END
                    WHERE status = 'pending'
                """)
            # DB에서 변경한 소통 기준은 재시작 후에도 유지한다.
            settings = await conn.fetch("SELECT key, value FROM bot_metadata WHERE key LIKE 'activity_%'")
            loaded = {row["key"]: row["value"] for row in settings}
            global MALE_GROUP_EMOJIS, FEMALE_GROUP_EMOJIS
            global MALE_DAILY_MIN_MESSAGES, FEMALE_DAILY_MIN_MESSAGES
            if "activity_male_emojis" in loaded:
                MALE_GROUP_EMOJIS = tuple(json.loads(loaded["activity_male_emojis"]))
            if "activity_female_emojis" in loaded:
                FEMALE_GROUP_EMOJIS = tuple(json.loads(loaded["activity_female_emojis"]))
            if set(MALE_GROUP_EMOJIS) & set(FEMALE_GROUP_EMOJIS):
                raise RuntimeError("DB의 남성/여성 이모티콘 설정이 중복됩니다. !소통설정으로 수정해 주세요.")
            if "activity_male_min" in loaded:
                MALE_DAILY_MIN_MESSAGES = max(1, int(loaded["activity_male_min"]))
            if "activity_female_min" in loaded:
                FEMALE_DAILY_MIN_MESSAGES = max(1, int(loaded["activity_female_min"]))
    except Exception:
        await db_pool.close()
        db_pool = None
        raise


async def record_event(
    event_id, user_id, chat_id, message_type, message_text, targets, unresolved,
    line_message_id=None, quoted_message_id=None, all_mentioned=False,
):
    """Store a message and create notifications for personal mentions, replies and @all."""
    now = iso_now()
    async with db_connect() as db:
        await db.execute("BEGIN")
        try:
            if event_id:
                cur = await db.execute(
                    "INSERT OR IGNORE INTO processed_events(event_id, processed_at) VALUES (?, ?)",
                    (event_id, now),
                )
                if cur.rowcount == 0:
                    await db.rollback()
                    return None
            is_new = False
            if user_id:
                cur = await db.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,))
                is_new = await cur.fetchone() is None
                await db.execute(
                    """INSERT INTO users(user_id, first_seen, last_seen, message_count)
                       VALUES (?, ?, ?, 1)
                       ON CONFLICT(user_id) DO UPDATE SET
                         last_seen=excluded.last_seen,
                         message_count=users.message_count+1""",
                    (user_id, now, now),
                )
            await db.execute(
                """INSERT INTO messages(event_id,line_message_id,user_id,chat_id,message_type,message_text,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (event_id, line_message_id, user_id, chat_id, message_type, message_text, now),
            )
            if message_type == "text" and user_id and message_text:
                # 개인 멘션
                for target_id in targets:
                    if target_id == user_id:
                        continue
                    await db.execute(
                        """INSERT INTO mentions(event_id,chat_id,sender_id,target_id,notification_type,original_text,created_at,status)
                           VALUES(?,?,?,?, 'mention', ?,?,'pending')""",
                        (event_id, chat_id, user_id, target_id, message_text, now),
                    )

                # 답장: 인용된 LINE 메시지 ID로 원 작성자를 찾아 알림을 만든다.
                if quoted_message_id:
                    quoted = await (await db.execute(
                        "SELECT user_id FROM messages WHERE chat_id=? AND line_message_id=? LIMIT 1",
                        (chat_id, str(quoted_message_id)),
                    )).fetchone()
                    reply_target = quoted["user_id"] if quoted else None
                    if reply_target and reply_target != user_id and reply_target not in targets:
                        await db.execute(
                            """INSERT INTO mentions(event_id,chat_id,sender_id,target_id,notification_type,original_text,created_at,status)
                               VALUES(?,?,?,?, 'reply', ?,?,'pending')""",
                            (event_id, chat_id, user_id, reply_target, message_text, now),
                        )

                # @all은 이 대화방에서 이전에 메시지를 보낸 사용자 각각에게 알림을 만든다.
                # LINE은 전체 참여자 명단을 항상 제공하지 않으므로, 봇이 관측한 참여자만 대상이 된다.
                if all_mentioned:
                    participants = await (await db.execute(
                        "SELECT DISTINCT user_id FROM messages WHERE chat_id=? AND user_id IS NOT NULL",
                        (chat_id,),
                    )).fetchall()
                    already_notified = set(targets)
                    if quoted_message_id:
                        # 위에서 답장 알림이 만들어졌더라도 @all은 중복 알림을 피한다.
                        quoted = await (await db.execute(
                            "SELECT user_id FROM messages WHERE chat_id=? AND line_message_id=? LIMIT 1",
                            (chat_id, str(quoted_message_id)),
                        )).fetchone()
                        if quoted and quoted["user_id"]:
                            already_notified.add(quoted["user_id"])
                    for participant in participants:
                        target_id = participant["user_id"]
                        if target_id == user_id or target_id in already_notified:
                            continue
                        await db.execute(
                            """INSERT INTO mentions(event_id,chat_id,sender_id,target_id,notification_type,original_text,created_at,status)
                               VALUES(?,?,?,?, 'all_mention', ?,?,'pending')""",
                            (event_id, chat_id, user_id, target_id, message_text, now),
                        )
                if unresolved:
                    await db.execute(
                        """INSERT INTO mentions(event_id,chat_id,sender_id,target_id,notification_type,original_text,created_at,status)
                           VALUES(?,?,?,NULL,'unresolved',?,?,'unresolved')""",
                        (event_id, chat_id, user_id, message_text, now),
                    )
            await db.commit()
            return {"is_new_user": is_new, "created_at": now}
        except Exception:
            await db.rollback()
            raise


def event_occurred_at(event) -> str:
    """LINE webhook의 밀리초 timestamp를 ISO UTC 문자열로 변환한다."""
    timestamp = field(event, "timestamp")
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp / 1000, timezone.utc).isoformat()
    return iso_now()


async def record_membership_event(event_id: str | None, chat_id: str, user_id: str,
                                  event_type: str, occurred_at: str) -> bool:
    async with db_connect() as db:
        if event_id:
            existing = await (await db.execute(
                "SELECT 1 FROM membership_events WHERE event_id=? AND user_id=? AND event_type=? LIMIT 1",
                (event_id, user_id, event_type),
            )).fetchone()
            if existing:
                return False
        await db.execute(
            "INSERT INTO membership_events(event_id,chat_id,user_id,event_type,occurred_at) VALUES(?,?,?,?,?)",
            (event_id, chat_id, user_id, event_type, occurred_at),
        )
        await db.commit()
    return True


async def membership_history(chat_id: str, user_id: str | None = None, limit: int = 30):
    async with db_connect() as db:
        if user_id:
            rows = await (await db.execute(
                """SELECT user_id,event_type,occurred_at FROM membership_events
                   WHERE chat_id=? AND user_id=? ORDER BY occurred_at DESC,id DESC LIMIT ?""",
                (chat_id, user_id, limit),
            )).fetchall()
            first = await (await db.execute(
                """SELECT MIN(occurred_at) AS first_join FROM membership_events
                   WHERE chat_id=? AND user_id=? AND event_type='joined'""",
                (chat_id, user_id),
            )).fetchone()
            last = await (await db.execute(
                """SELECT MAX(occurred_at) AS last_left FROM membership_events
                   WHERE chat_id=? AND user_id=? AND event_type='left'""",
                (chat_id, user_id),
            )).fetchone()
            return rows, (first["first_join"] if first else None), (last["last_left"] if last else None)
        rows = await (await db.execute(
            """SELECT user_id,event_type,occurred_at FROM membership_events
               WHERE chat_id=? ORDER BY occurred_at DESC,id DESC LIMIT ?""",
            (chat_id, limit),
        )).fetchall()
        return rows, None, None


async def claim_pending_mentions(chat_id: str, target_id: str, limit: int):
    """Atomically reserve a batch so concurrent webhook handlers don't both send it."""
    token = os.urandom(16).hex()
    now = iso_now()
    stale_before = (utc_now() - timedelta(minutes=3)).isoformat()
    async with db_connect() as db:
        await db.execute("BEGIN")
        try:
            # Recover claims stranded by a crashed worker. This is at-least-once, not exactly-once.
            await db.execute(
                """UPDATE mentions SET status='pending', processing_token=NULL, processing_since=NULL
                   WHERE status='processing' AND processing_since < ?
                     AND chat_id=? AND target_id=?""",
                (stale_before, chat_id, target_id),
            )
            rows = await (await db.execute(
                """SELECT id,sender_id,notification_type,original_text,created_at FROM mentions
                   WHERE chat_id=? AND target_id=? AND status='pending'
                   ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED""",
                (chat_id, target_id, limit),
            )).fetchall()
            ids = [r["id"] for r in rows]
            if ids:
                marks = ",".join("?" for _ in ids)
                await db.execute(
                    f"""UPDATE mentions SET status='processing', processing_token=?, processing_since=?
                        WHERE id IN ({marks}) AND status='pending'""",
                    (token, now, *ids),
                )
            await db.commit()
            return token, rows
        except Exception:
            await db.rollback()
            raise


async def finish_claim(mention_ids, token, delivered: bool):
    if not mention_ids:
        return
    marks = ",".join("?" for _ in mention_ids)
    if delivered:
        sql = f"""UPDATE mentions SET status='delivered', delivered_at=?, processing_token=NULL, processing_since=NULL
                   WHERE id IN ({marks}) AND status='processing' AND processing_token=?"""
        params = (iso_now(), *mention_ids, token)
    else:
        sql = f"""UPDATE mentions SET status='pending', processing_token=NULL, processing_since=NULL
                   WHERE id IN ({marks}) AND status='processing' AND processing_token=?"""
        params = (*mention_ids, token)
    async with db_connect() as db:
        await db.execute(sql, params)
        await db.commit()


async def statistics(chat_id: str | None = None, day_offset: int = 0):
    """Return cumulative totals plus the selected local day's activity and ranking."""
    local_today = datetime.now(ZoneInfo(TIMEZONE_NAME)).date()
    selected_date = local_today - timedelta(days=day_offset)
    local_start = datetime.combine(selected_date, datetime.min.time(), tzinfo=ZoneInfo(TIMEZONE_NAME))
    start_utc = local_start.astimezone(timezone.utc).isoformat()
    end_utc = (local_start + timedelta(days=1)).astimezone(timezone.utc).isoformat()
    where = " WHERE chat_id=?" if chat_id else ""
    base_args = (chat_id,) if chat_id else ()
    aggregate_sql = f"""SELECT COUNT(*) AS total,
        COUNT(*) FILTER (WHERE created_at >= ? AND created_at < ?) AS day_count,
        COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL AND created_at >= ? AND created_at < ?) AS users
        FROM messages{where}"""
    aggregate_args = (start_utc, end_utc, start_utc, end_utc, *base_args)
    top_sql = f"""SELECT user_id, COUNT(*) AS count FROM messages{where}
                  {'AND' if where else 'WHERE'} user_id IS NOT NULL AND created_at >= ? AND created_at < ?
                  GROUP BY user_id ORDER BY count DESC, user_id LIMIT 10"""
    top_args = (*base_args, start_utc, end_utc)
    async with db_connect() as db:
        aggregate = await (await db.execute(aggregate_sql, aggregate_args)).fetchone()
        top = await (await db.execute(top_sql, top_args)).fetchall()
    return {"total": aggregate["total"], "day_count": aggregate["day_count"],
            "users": aggregate["users"], "top": top, "date": selected_date.isoformat(),
            "day_offset": day_offset}


async def format_stats(title, stats, source_type=None, chat_raw_id=None, api=None):
    lines = [
        f"📊 {title} · {stats['date']}",
        "━━━━━━━━━━━━━━",
        f"💬 누적 메시지  {stats['total']:,}개",
        f"📅 해당 날짜 메시지  {stats['day_count']:,}개",
        f"👥 해당 날짜 활동 사용자  {stats['users']:,}명",
        f"🎯 그룹별 하루 최소 소통량: 🔭 {MALE_DAILY_MIN_MESSAGES:,}개 / 💫·📡 {FEMALE_DAILY_MIN_MESSAGES:,}개",
        "",
        "🏆 해당 날짜 활동 TOP 10  (그룹별 달성 여부)",
    ]
    if not stats["top"]:
        lines.append("해당 날짜에 집계된 메시지가 없어요.")
    else:
        medals = ["🥇", "🥈", "🥉"]
        for i, row in enumerate(stats["top"]):
            user_id = row["user_id"]
            name = None
            if api is not None:
                name = await display_name_for(source_type or "user", chat_raw_id, user_id, api)
            classification = classify_user_group(name)
            medal = medals[i] if i < len(medals) else f"{i + 1}."
            if classification is None:
                lines.append(f"{medal} {name or user_id}  ·  {row['count']:,}개 (그룹 미지정 · 판정 제외)")
            else:
                group_name, threshold = classification
                achieved = "달성" if int(row["count"]) >= threshold else "미달성"
                lines.append(
                    f"{medal} {name or user_id}  ·  {row['count']:,}개 / {threshold:,}개 "
                    f"({group_name} · {achieved})"
                )
    return "\n".join(lines)


async def warning_count(chat_id: str, target_id: str) -> int:
    async with db_connect() as db:
        row = await (await db.execute(
            "SELECT COUNT(*) AS n FROM moderation_warnings WHERE chat_id=? AND target_id=? AND active=TRUE",
            (chat_id, target_id),
        )).fetchone()
    return int(row["n"])


async def low_activity_users(chat_id: str, day_offset: int = 0):
    """해당 날짜의 소통량을 그룹별 기준으로 판정해 미달 사용자 목록을 반환한다."""
    local_today = datetime.now(ZoneInfo(TIMEZONE_NAME)).date()
    selected_date = local_today - timedelta(days=day_offset)
    local_start = datetime.combine(selected_date, datetime.min.time(), tzinfo=ZoneInfo(TIMEZONE_NAME))
    start_utc = local_start.astimezone(timezone.utc).isoformat()
    end_utc = (local_start + timedelta(days=1)).astimezone(timezone.utc).isoformat()
    async with db_connect() as db:
        rows = await (await db.execute(
            """WITH participants AS (
                   SELECT DISTINCT user_id FROM messages
                   WHERE chat_id=? AND user_id IS NOT NULL
               ), daily AS (
                   SELECT user_id, COUNT(*) AS day_count
                   FROM messages
                   WHERE chat_id=? AND user_id IS NOT NULL
                     AND created_at>=? AND created_at<?
                   GROUP BY user_id
               )
               SELECT participants.user_id, COALESCE(daily.day_count, 0) AS day_count
               FROM participants LEFT JOIN daily USING(user_id)
               ORDER BY day_count ASC, participants.user_id""",
            (chat_id, chat_id, start_utc, end_utc),
        )).fetchall()
    return selected_date.isoformat(), rows


async def display_name_for(source_type: str, chat_raw_id: str | None, user_id: str | None, api):
    """Return a cached display name, refreshing it at most once per day when possible."""
    if not user_id:
        return None

    # Profile requests are comparatively slow and rate-limited; reuse a fresh DB value.
    async with db_connect() as db:
        cached = await (await db.execute(
            "SELECT display_name, display_name_updated_at FROM users WHERE user_id=?",
            (user_id,),
        )).fetchone()
    if cached and cached["display_name"] and cached["display_name_updated_at"]:
        try:
            updated_at = datetime.fromisoformat(cached["display_name_updated_at"])
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            if utc_now() - updated_at < timedelta(hours=24):
                return cached["display_name"]
        except (ValueError, TypeError):
            pass

    try:
        if source_type == "group" and chat_raw_id:
            profile = await api.get_group_member_profile(chat_raw_id, user_id)
        elif source_type == "room" and chat_raw_id:
            profile = await api.get_room_member_profile(chat_raw_id, user_id)
        else:
            profile = await api.get_profile(user_id)
        name = field(profile, "display_name", "displayName")
        if name:
            async with db_connect() as db:
                await db.execute(
                    "UPDATE users SET display_name=?, display_name_updated_at=? WHERE user_id=?",
                    (name, iso_now(), user_id),
                )
                await db.commit()
            return name
    except Exception as exc:
        log.debug("사용자 %s의 프로필을 조회할 수 없습니다: %s", user_id, exc)
    return cached["display_name"] if cached and cached["display_name"] else None


async def cleanup_old_data():
    """Apply retention to message text and delivered mention records; 0 disables cleanup."""
    if RETENTION_DAYS <= 0:
        return
    cutoff = (utc_now() - timedelta(days=RETENTION_DAYS)).isoformat()
    async with db_connect() as db:
        # Keep aggregate user counts, but delete detailed message history after retention.
        await db.execute("DELETE FROM messages WHERE created_at < ?", (cutoff,))
        await db.execute("DELETE FROM mentions WHERE created_at < ? AND status IN ('delivered','unresolved')", (cutoff,))
        await db.execute("DELETE FROM processed_events WHERE processed_at < ?", (cutoff,))
        await db.commit()


async def cleanup_loop():
    while True:
        try:
            await cleanup_old_data()
        except Exception:
            log.exception("예약된 데이터 정리 작업에 실패했습니다.")
        await asyncio.sleep(24 * 60 * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    client = AsyncApiClient(configuration)
    app.state.line_api = AsyncMessagingApi(client)
    task = asyncio.create_task(cleanup_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await client.close()
        if db_pool is not None:
            await db_pool.close()


app = FastAPI(title="LINE 커뮤니티 봇", version="1.0.0", lifespan=lifespan)


@app.get("/")
async def health():
    return {"status": "ok", "service": "line-community-bot"}


@app.get("/health/db")
async def health_db():
    async with db_connect() as db:
        await (await db.execute("SELECT 1")).fetchone()
    return {"status": "ok", "database": "ok"}


@app.get("/exports/{token}")
async def download_database_export(token: str):
    """유효한 임시 토큰으로 JSON 백업 파일을 내려받는다."""
    cleanup_expired_exports()
    item = export_tokens.get(token)
    if not item:
        raise HTTPException(status_code=404, detail="다운로드 링크가 만료되었거나 유효하지 않습니다.")
    file_path, expires_at = item
    if expires_at <= time.time() or not Path(file_path).is_file():
        export_tokens.pop(token, None)
        Path(file_path).unlink(missing_ok=True)
        raise HTTPException(status_code=404, detail="다운로드 링크가 만료되었거나 유효하지 않습니다.")
    return FileResponse(file_path, media_type="application/json", filename="데이터베이스_내보내기.json")


@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature")
    if not signature:
        raise HTTPException(status_code=400, detail="LINE 서명 헤더가 없습니다.")
    body_bytes = await request.body()
    try:
        events = parser.parse(body_bytes.decode("utf-8"), signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="LINE 서명이 올바르지 않습니다.")
    except Exception:
        log.exception("웹훅 요청 본문을 해석하지 못했습니다.")
        raise HTTPException(status_code=400, detail="웹훅 요청 형식이 올바르지 않습니다.")

    api = request.app.state.line_api
    for event in events:
        event_type = str(field(event, "type", default="") or "")
        if event_type in {"memberJoined", "memberLeft"}:
            source = field(event, "source")
            _, chat_id, source_type = source_identity(source)
            raw_chat_id = field(source, "group_id", "groupId", "room_id", "roomId")
            if not chat_id or not raw_chat_id:
                log.info("대화방 식별 정보가 없어 입퇴장 이벤트를 건너뜁니다.")
                continue
            membership_type = "joined" if event_type == "memberJoined" else "left"
            event_id = field(event, "webhook_event_id", "webhookEventId")
            occurred_at = event_occurred_at(event)
            membership_payload = field(event, "joined" if membership_type == "joined" else "left")
            members = field(membership_payload, "members", default=[]) or []
            left_names = []
            for member in members:
                member_id = field(member, "user_id", "userId")
                if not member_id:
                    continue
                try:
                    inserted = await record_membership_event(
                        event_id, chat_id, str(member_id), membership_type, occurred_at
                    )
                except Exception:
                    log.exception("입퇴장 이벤트를 저장하지 못했습니다.")
                    raise HTTPException(status_code=500, detail="입퇴장 기록 저장 중 오류가 발생했습니다.")
                if inserted and membership_type == "left":
                    name = await display_name_for(source_type, raw_chat_id, str(member_id), api)
                    left_names.append(name or str(member_id))
            # 입장 알림은 보내지 않고, 퇴장 알림만 해당 대화방에 전송한다.
            if membership_type == "left" and left_names:
                names_text = "、".join(left_names)
                try:
                    await api.push_message(PushMessageRequest(
                        to=raw_chat_id,
                        messages=[TextMessage(text=f"👋 퇴장 알림\n{names_text}님이 대화방을 나갔어요.")],
                    ))
                except Exception:
                    log.exception("대화방 %s에 퇴장 알림을 전송하지 못했습니다.", chat_id)
            continue

        if not isinstance(event, MessageEvent):
            continue
        source = event.source
        message = event.message
        user_id, chat_id, source_type = source_identity(source)
        if not chat_id:
            log.info("대화방 식별 정보가 없어 이벤트를 건너뜁니다.")
            continue
        message_type = field(message, "type", default="unknown")
        text = field(message, "text") if message_type == "text" else None
        event_id = field(event, "webhook_event_id", "webhookEventId")
        targets, unresolved, all_mentioned = mention_targets(message) if message_type == "text" else ([], 0, False)
        line_message_id = field(message, "id")
        quoted_message_id = field(message, "quoted_message_id", "quotedMessageId")
        try:
            result = await record_event(
                event_id, user_id, chat_id, message_type, text, targets, unresolved,
                line_message_id=line_message_id, quoted_message_id=quoted_message_id,
                all_mentioned=all_mentioned,
            )
        except Exception:
            log.exception("이벤트를 데이터베이스에 저장하지 못했습니다.")
            # Non-2xx encourages LINE retry. Idempotency prevents duplicate DB rows on retry.
            raise HTTPException(status_code=500, detail="이벤트 저장 중 오류가 발생했습니다.")
        if result is None:
            continue
        if message_type != "text" or not text:
            continue

        stripped = text.strip()
        reply_parts: list[str] = []
        claim = None
        is_admin = bool(user_id and user_id in ADMIN_USER_IDS)

        command = COMMAND_ALIASES.get(stripped.casefold())
        stats_match = re.fullmatch(r"!통계\s+-(\d+)", stripped)
        stats_day_offset = int(stats_match.group(1)) if stats_match else 0
        if stats_match:
            command = "stats day"
        elif re.match(r"^!경고(?:\s|$)", stripped):
            command = "warnings"
        low_activity_match = re.fullmatch(r"!소통미달(?:\s+-(\d+))?", stripped)
        low_activity_offset = int(low_activity_match.group(1) or 0) if low_activity_match else 0
        if low_activity_match:
            command = "low_activity"
        membership_match = re.fullmatch(r"!입퇴장(?:\s+(.+))?", stripped)
        if membership_match:
            command = "membership"
        activity_settings_match = re.fullmatch(r"!소통설정(?:\s+(.+))?", stripped)
        activity_settings_argument = activity_settings_match.group(1).strip() if activity_settings_match and activity_settings_match.group(1) else ""
        if activity_settings_match:
            command = "activity_settings"

        # 명령어 처리: 답장은 아래 공통 처리 구간에서 한 번에 전송한다.
        if command == "help":
            reply_parts.append(
                "📖 봇 명령어 안내\n"
                "━━━━━━━━━━━━━━\n"
                "• !도움말 — 명령어 목록 보기\n\n"
                "📊 통계\n"
                "• !통계 — 현재 대화방 통계\n"
                "• !통계 대화방 — 현재 대화방 통계\n"
                "• !통계 나 — 내 활동 기록\n"
                "• !통계 전체 — 전체 통계 (관리자 전용)\n"
                "• !통계 -1 — 어제 통계, !통계 -2 — 이틀 전 통계\n\n"
                "⚖️ 경고 관리 (관리자 전용)\n"
                "• !경고 @사용자 — 경고 부여\n"
                "• !경고 취소 @사용자 — 경고 1회 취소\n"
                "• !경고 — 현재 대화방 경고 현황\n"
                "• !경고주의 — 경고 기준 도달 사용자 확인\n"
                "• !소통미달 — 오늘 최소 소통량 미달 사용자 (관리자 전용)\n"
                "• !소통미달 -1 — 어제 최소 소통량 미달 사용자\n"
                "• !소통설정 — 현재 기준 확인 (관리자 전용)\n"
                "• !소통설정 남성기준 90 — 남성 기준 변경\n"
                "• !소통설정 여성기준 70 — 여성 기준 변경\n"
                "• !소통설정 남성이모티콘 🔭,🛰️ — 남성 이모티콘 그룹 변경\n"
                "• !소통설정 여성이모티콘 💫,📡 — 여성 이모티콘 그룹 변경\n"
                f"• 현재 기준 — 🔭 남성 {MALE_DAILY_MIN_MESSAGES}개 / 💫·📡 여성 {FEMALE_DAILY_MIN_MESSAGES}개\n\n"
                "📨 멘션·답장 알림\n"
                "• 나를 멘션하거나 내 메시지에 답장하면, 내가 다음에 일반 채팅을 입력할 때 알림 내용이 자동으로 표시돼요.\n"
                "• !멘션 — 자동 표시를 기다리지 않고 새 멘션·답장·전체 멘션 기록 확인\n"
                "※ 명령어(!로 시작하는 메시지)는 자동 알림 표시를 실행하지 않아요.\n\n"
                "👥 입퇴장 기록 (관리자 전용)\n"
                "• !입퇴장 — 현재 대화방의 최근 입퇴장 기록\n"
                "• !입퇴장 전체 — 현재 대화방의 최근 입퇴장 기록 목록\n"
                "• !입퇴장 사용자ID 또는 !입퇴장 @사용자 — 해당 사용자의 최초 입장 시각, 마지막 퇴장 시각 및 기록 확인\n"
                "※ 퇴장 시에만 대화방에 알림을 보내며, 입장 알림은 보내지 않아요.\n\n"
                "🛠️ 관리 기능\n"
                "• !데이터베이스 — 데이터베이스 JSON 내보내기 (관리자 전용)\n\n"
                "명령어를 대화방에 입력하면 사용할 수 있어요.\n영문 명령어도 계속 사용할 수 있어요."
            )
        elif command in {"stats", "stats group", "stats day"}:
            raw_chat_id = field(source, "group_id", "groupId", "room_id", "roomId")
            offset = stats_day_offset if command == "stats day" else 0
            if offset > 365:
                reply_parts.append("❌ 최대 365일 전까지 조회할 수 있어요.\n예: !통계 -7")
            else:
                label = "현재 대화방 통계" if command != "stats day" else f"현재 대화방 {offset}일 전 통계"
                reply_parts.append(await format_stats(
                    label, await statistics(chat_id, offset), source_type, raw_chat_id, api
                ))
        elif command == "stats all":
            if is_admin:
                reply_parts.append(await format_stats("전체 통계", await statistics(), "user", None, api))
            else:
                reply_parts.append("🔒 관리자 전용 명령어예요.\n이 기능은 관리자만 사용할 수 있어요.")
        elif command == "stats me":
            if not user_id:
                reply_parts.append("사용자 정보를 확인할 수 없어 통계를 표시할 수 없어요.")
            else:
                async with db_connect() as db:
                    row = await (await db.execute(
                        "SELECT COUNT(*) FROM messages WHERE user_id=?", (user_id,)
                    )).fetchone()
                    first = await (await db.execute(
                        "SELECT first_seen,last_seen FROM users WHERE user_id=?", (user_id,)
                    )).fetchone()
                reply_parts.append(
                    "👤 내 활동 기록\n"
                    "━━━━━━━━━━━━━━\n"
                    f"💬 누적 메시지  {row[0]:,}개\n"
                    f"🌱 첫 기록  {format_datetime(first['first_seen'] if first else None)}\n"
                    f"🕒 최근 활동  {format_datetime(first['last_seen'] if first else None)}"
                )
        elif command == "mentions":
            if not user_id:
                reply_parts.append("사용자 정보를 확인할 수 없어 멘션 기록을 표시할 수 없어요.")
            else:
                token, rows = await claim_pending_mentions(chat_id, user_id, MENTION_BATCH_LIMIT)
                if rows:
                    lines = ["📨 나에게 온 멘션·답장 기록", "━━━━━━━━━━━━━━"]
                    raw_chat_id = field(source, "group_id", "groupId", "room_id", "roomId")
                    for row in rows:
                        sender_name = await display_name_for(source_type, raw_chat_id, row["sender_id"], api)
                        type_label = {"mention": "📌 멘션", "reply": "💬 답장", "all_mention": "📣 전체 멘션"}.get(row["notification_type"], "📨 알림")
                        lines.append(
                            f"\n{type_label}"
                            f"\n👤 보낸 사람: {sender_name or row['sender_id']}"
                            f"\n📝 내용: {row['original_text']}"
                            f"\n🕒 시간: {format_datetime(row['created_at'])}"
                        )
                    reply_parts.append("\n".join(lines))
                    claim = (token, [r["id"] for r in rows])
                else:
                    reply_parts.append("📭 현재 대화방에서 새로 확인할 멘션이나 답장이 없어요.")
        elif command == "activity_settings":
            if not is_admin:
                reply_parts.append("🔒 소통 기준 설정은 관리자만 사용할 수 있어요.")
            elif not activity_settings_argument:
                reply_parts.append(
                    "⚙️ 현재 소통 기준 설정\n━━━━━━━━━━━━━━\n"
                    f"🔭 남성 이모티콘: {', '.join(MALE_GROUP_EMOJIS)}\n"
                    f"남성 하루 기준: {MALE_DAILY_MIN_MESSAGES}개\n\n"
                    f"💫 여성 이모티콘: {', '.join(FEMALE_GROUP_EMOJIS)}\n"
                    f"여성 하루 기준: {FEMALE_DAILY_MIN_MESSAGES}개\n\n"
                    "변경 명령어:\n"
                    "!소통설정 남성기준 90\n!소통설정 여성기준 70\n"
                    "!소통설정 남성이모티콘 🔭,🛰️\n!소통설정 여성이모티콘 💫,📡"
                )
            else:
                parts = activity_settings_argument.split(maxsplit=1)
                setting_key = parts[0].casefold()
                value = parts[1].strip() if len(parts) > 1 else ""
                key_map = {
                    "남성기준": ("activity_male_min", "male_min"),
                    "여성기준": ("activity_female_min", "female_min"),
                    "남성이모티콘": ("activity_male_emojis", "male_emojis"),
                    "여성이모티콘": ("activity_female_emojis", "female_emojis"),
                }
                if setting_key not in key_map or not value:
                    reply_parts.append("❌ 사용법을 확인해 주세요. 예: !소통설정 남성기준 90")
                else:
                    metadata_key, kind = key_map[setting_key]
                    try:
                        if kind.endswith("_min"):
                            if not value.isdigit() or not 1 <= int(value) <= 100000:
                                raise ValueError("기준은 1~100000 사이의 정수여야 합니다.")
                            new_value = int(value)
                        else:
                            emojis = tuple(dict.fromkeys(x.strip() for x in value.split(",") if x.strip()))
                            if not emojis or len(emojis) > 20 or any(len(x) > 16 for x in emojis):
                                raise ValueError("이모티콘은 쉼표로 구분해 1~20개 입력해 주세요.")
                            other = FEMALE_GROUP_EMOJIS if kind == "male_emojis" else MALE_GROUP_EMOJIS
                            if set(emojis) & set(other):
                                raise ValueError("남성/여성 그룹에 중복된 이모티콘을 지정할 수 없어요.")
                            new_value = emojis
                        stored_value = json.dumps(list(new_value), ensure_ascii=False) if kind.endswith("_emojis") else str(new_value)
                        async with db_connect() as db:
                            await db.execute(
                                "INSERT INTO bot_metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                                (metadata_key, stored_value),
                            )
                            await db.commit()
                        if kind == "male_min":
                            globals()["MALE_DAILY_MIN_MESSAGES"] = new_value
                        elif kind == "female_min":
                            globals()["FEMALE_DAILY_MIN_MESSAGES"] = new_value
                        elif kind == "male_emojis":
                            globals()["MALE_GROUP_EMOJIS"] = new_value
                        elif kind == "female_emojis":
                            globals()["FEMALE_GROUP_EMOJIS"] = new_value
                        reply_parts.append(f"✅ 소통 설정을 저장했어요.\n{setting_key}: {stored_value if kind.endswith('_emojis') else new_value}")
                    except ValueError as exc:
                        reply_parts.append(f"❌ 설정을 변경하지 못했어요. {exc}")
                    except Exception:
                        log.exception("소통 설정 저장 실패")
                        reply_parts.append("❌ 설정 저장에 실패했어요. 데이터베이스 연결을 확인해 주세요.")
        elif command == "low_activity":
            if not is_admin:
                reply_parts.append("🔒 소통량 관리 명령어는 관리자만 사용할 수 있어요.")
            elif low_activity_offset > 365:
                reply_parts.append("❌ 최대 365일 전까지 조회할 수 있어요.\n예: !소통미달 -7")
            else:
                selected_date, rows = await low_activity_users(chat_id, low_activity_offset)
                raw_chat_id = field(source, "group_id", "groupId", "room_id", "roomId")
                title = f"📉 {selected_date} 소통량 미달 사용자"
                lines = [
                    title, "━━━━━━━━━━━━━━",
                    f"기준: 🔭 남성 {MALE_DAILY_MIN_MESSAGES:,}개 / 💫·📡 여성 {FEMALE_DAILY_MIN_MESSAGES:,}개 미만", ""
                ]
                below_threshold = []
                for row in rows:
                    name = await display_name_for(source_type, raw_chat_id, row["user_id"], api)
                    classification = classify_user_group(name)
                    if classification is None:
                        continue
                    group_name, threshold = classification
                    count = int(row["day_count"])
                    if count < threshold:
                        below_threshold.append((row, name, group_name, threshold, count))
                    if len(below_threshold) >= 50:
                        break
                if not below_threshold:
                    lines.append("✅ 확인된 참여자 중 소통량 미달 사용자가 없어요.")
                else:
                    for row, name, group_name, threshold, count in below_threshold:
                        lines.append(
                            f"• {name or row['user_id']} — {count:,}개 / {threshold:,}개 "
                            f"({group_name})"
                        )
                    if len(below_threshold) >= 50:
                        lines.append("\n※ 표시 가능한 최대 50명까지만 보여 줬어요.")
                reply_parts.append("\n".join(lines))
        elif command == "warning_alerts":
            if not is_admin:
                reply_parts.append("🔒 경고 관리 명령어는 관리자만 사용할 수 있어요.")
            else:
                async with db_connect() as db:
                    rows = await (await db.execute(
                        """SELECT target_id, COUNT(*) AS n FROM moderation_warnings
                           WHERE chat_id=? AND active=TRUE GROUP BY target_id
                           HAVING COUNT(*) >= ? ORDER BY n DESC, target_id LIMIT 50""",
                        (chat_id, WARNING_ALERT_THRESHOLD),
                    )).fetchall()
                raw_chat_id = field(source, "group_id", "groupId", "room_id", "roomId")
                lines = ["🚨 경고 기준 도달 사용자", "━━━━━━━━━━━━━━", f"주의 기준: 경고 {WARNING_ALERT_THRESHOLD}회 이상", ""]
                if not rows:
                    lines.append("현재 기준에 도달한 사용자가 없어요.")
                else:
                    for row in rows:
                        name = await display_name_for(source_type, raw_chat_id, row["target_id"], api)
                        lines.append(f"🚨 {name or row['target_id']} — 경고 {int(row['n'])}회 · 관리자 확인 필요")
                reply_parts.append("\n".join(lines))
        elif command == "warnings":
            if not is_admin:
                reply_parts.append("🔒 경고 관리 명령어는 관리자만 사용할 수 있어요.")
            elif stripped == "!경고":
                async with db_connect() as db:
                    rows = await (await db.execute(
                        """SELECT target_id, COUNT(*) AS n FROM moderation_warnings
                           WHERE chat_id=? AND active=TRUE GROUP BY target_id ORDER BY n DESC, target_id LIMIT 30""",
                        (chat_id,),
                    )).fetchall()
                if not rows:
                    reply_parts.append("✅ 현재 대화방에 활성 경고가 없어요.")
                else:
                    raw_chat_id = field(source, "group_id", "groupId", "room_id", "roomId")
                    lines = ["⚖️ 현재 대화방 경고 현황", "━━━━━━━━━━━━━━"]
                    for row in rows:
                        name = await display_name_for(source_type, raw_chat_id, row["target_id"], api)
                        count = int(row["n"])
                        marker = " · 🚨 관리자 확인 필요" if count >= WARNING_ALERT_THRESHOLD else ""
                        lines.append(f"• {name or row['target_id']} — {count}회{marker}")
                    reply_parts.append("\n".join(lines))
            else:
                cancel = bool(re.match(r"^!경고\s+취소(?:\s|$)", stripped))
                targets_for_warning = [target for target in targets if target != user_id]
                if len(targets_for_warning) != 1:
                    reply_parts.append("사용법: !경고 @사용자 또는 !경고 취소 @사용자\n대상 사용자를 LINE 멘션으로 지정해 주세요.")
                else:
                    target_id = targets_for_warning[0]
                    if cancel:
                        async with db_connect() as db:
                            row = await (await db.execute(
                                """SELECT id FROM moderation_warnings WHERE chat_id=? AND target_id=? AND active=TRUE
                                   ORDER BY id DESC LIMIT 1""",
                                (chat_id, target_id),
                            )).fetchone()
                            if row:
                                await db.execute(
                                    "UPDATE moderation_warnings SET active=FALSE,cancelled_by=?,cancelled_at=? WHERE id=?",
                                    (user_id, iso_now(), row["id"]),
                                )
                                await db.commit()
                        name = await display_name_for(source_type, field(source, "group_id", "groupId", "room_id", "roomId"), target_id, api)
                        remaining = await warning_count(chat_id, target_id)
                        reply_parts.append((f"✅ {name or target_id}님의 경고 1회를 취소했어요." if row else f"ℹ️ {name or target_id}님에게 취소할 활성 경고가 없어요.") + f"\n현재 경고: {remaining}회")
                    else:
                        async with db_connect() as db:
                            await db.execute(
                                "INSERT INTO moderation_warnings(chat_id,target_id,issuer_id,created_at) VALUES(?,?,?,?)",
                                (chat_id, target_id, user_id, iso_now()),
                            )
                            await db.commit()
                        name = await display_name_for(source_type, field(source, "group_id", "groupId", "room_id", "roomId"), target_id, api)
                        count = await warning_count(chat_id, target_id)
                        alert = f"\n🚨 경고 {WARNING_ALERT_THRESHOLD}회 이상: 관리자 확인이 필요해요." if count >= WARNING_ALERT_THRESHOLD else ""
                        reply_parts.append(f"⚠️ {name or target_id}님에게 경고를 부여했어요.\n현재 경고: {count}회{alert}")
        elif command == "membership":
            if not is_admin:
                reply_parts.append("🔒 입퇴장 기록은 관리자만 조회할 수 있어요.")
            else:
                match = re.fullmatch(r"!입퇴장(?:\s+(.+))?", stripped)
                argument = (match.group(1).strip() if match and match.group(1) else "")
                raw_chat_id = field(source, "group_id", "groupId", "room_id", "roomId")
                # !입퇴장 @사용자 형태를 지원한다. LINE의 mention 메타데이터에서
                # 실제 user_id를 가져오므로 표시 이름이나 텍스트의 @문자에 의존하지 않는다.
                if targets and len(targets) > 1 and argument and argument != "전체":
                    reply_parts.append("한 번에 한 명만 멘션해 주세요. 예: !입퇴장 @사용자")
                else:
                    if targets and argument and argument != "전체":
                        target_id = targets[0]
                    else:
                        target_id = None if not argument or argument == "전체" else argument
                    rows, first_join, last_left = await membership_history(chat_id, target_id, 30)
                    if target_id:
                        name = await display_name_for(source_type, raw_chat_id, target_id, api)
                        lines = [f"👤 사용자 입퇴장 기록: {name or target_id}", "━━━━━━━━━━━━━━",
                                 f"🆔 사용자 ID: {target_id}",
                                 f"🌱 최초 입장(봇 기록 기준): {format_datetime(first_join)}",
                                 f"👋 마지막 퇴장: {format_datetime(last_left)}", "", "최근 기록:"]
                    else:
                        lines = ["👥 현재 대화방 최근 입퇴장 기록 (관리자 전용)", "━━━━━━━━━━━━━━", ""]
                    if not rows:
                        lines.append("기록이 없어요. 봇이 입퇴장 이벤트를 받기 시작한 이후의 기록만 조회할 수 있어요.")
                    else:
                        for row in rows:
                            member_id = row["user_id"]
                            member_name = await display_name_for(source_type, raw_chat_id, member_id, api)
                            label = "입장" if row["event_type"] == "joined" else "퇴장"
                            lines.append(f"• {format_datetime(row['occurred_at'])} — {label} — {member_name or member_id}\n  ID: {member_id}")
                    if not target_id:
                        lines.append("\n특정 사용자 조회: !입퇴장 사용자ID 또는 !입퇴장 @사용자")
                    reply_parts.append("\n".join(lines)[:MAX_REPLY_CHARS])
        elif command == "db":
            if not is_admin:
                reply_parts.append("🔒 관리자 전용 명령어예요.\n이 기능은 관리자만 사용할 수 있어요.")
            else:
                try:
                    download_url = await create_database_export()
                    reply_parts.append(
                        "📦 데이터베이스 내보내기 완료\n"
                        "━━━━━━━━━━━━━━\n"
                        f"🔗 다운로드 링크\n{download_url}\n\n"
                        f"⏳ 링크는 {EXPORT_TTL // 60}분 동안 유효해요.\n"
                        "⚠️ 데이터가 포함된 링크이므로 다른 사람과 공유하지 마세요."
                    )
                except Exception:
                    log.exception("DB JSON 내보내기 실패")
                    reply_parts.append(
                        "❌ 데이터베이스 내보내기에 실패했어요.\n"
                        "잠시 후 다시 시도해 주세요.\n"
                        "문제가 계속되면 관리자에게 문의해 주세요."
                    )

        if ECHO_MESSAGES and not stripped.startswith("!"):
            reply_parts.insert(0, text)

        # On any ordinary user message, include a limited batch of pending mentions.
        if user_id and not stripped.startswith("!"):
            token, rows = await claim_pending_mentions(chat_id, user_id, MENTION_BATCH_LIMIT)
            if rows:
                claim = (token, [r["id"] for r in rows])
                lines = ["📨 나에게 온 멘션·답장 기록", "━━━━━━━━━━━━━━"]
                for row in rows:
                    sender_name = await display_name_for(
                        source_type,
                        field(source, "group_id", "groupId", "room_id", "roomId"),
                        row["sender_id"], api,
                    )
                    type_label = {"mention": "📌 멘션", "reply": "💬 답장", "all_mention": "📣 전체 멘션"}.get(row["notification_type"], "📨 알림")
                    lines.append(
                        f"\n{type_label}"
                        f"\n👤 보낸 사람: {sender_name or row['sender_id']}"
                        f"\n📝 내용: {row['original_text']}"
                        f"\n🕒 시간: {format_datetime(row['created_at'])}"
                    )
                reply_parts.append("\n".join(lines))

        if not reply_parts:
            # New user greeting is intentionally opt-in to avoid unwanted spam.
            continue
        reply_text = "\n\n".join(reply_parts)[:MAX_REPLY_CHARS]
        try:
            await api.reply_message(ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)],
            ))
        except Exception:
            log.exception("대화방 %s의 이벤트 %s에 답장하지 못했습니다.", chat_id, event_id)
            if claim:
                await finish_claim(claim[1], claim[0], delivered=False)
            continue
        if claim:
            await finish_claim(claim[1], claim[0], delivered=True)

    return {"status": "ok"}


@app.post("/admin/cleanup")
async def admin_cleanup(request: Request):
    """Manual retention cleanup; requires X-Admin-Token matching ADMIN_API_TOKEN."""
    expected = os.getenv("ADMIN_API_TOKEN", "")
    supplied = request.headers.get("X-Admin-Token", "")
    if not expected or not supplied or not __import__("hmac").compare_digest(expected, supplied):
        raise HTTPException(status_code=403, detail="관리자 인증에 실패했습니다.")
    await cleanup_old_data()
    return {"status": "ok", "retention_days": RETENTION_DAYS}
