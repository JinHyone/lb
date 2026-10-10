from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
import json
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import aiosqlite
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    AsyncApiClient,
    AsyncMessagingApi,
    Configuration,
    ReplyMessageRequest,
    TextMessage,
)
from linebot.v3.webhook import WebhookParser
from linebot.v3.webhooks import MessageEvent

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("DATABASE_PATH", str(BASE_DIR / "bot.db")))
CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
ADMIN_USER_IDS = {x.strip() for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()}
ECHO_MESSAGES = os.getenv("ECHO_MESSAGES", "false").lower() in {"1", "true", "yes"}
TIMEZONE_NAME = os.getenv("TIMEZONE", "Asia/Seoul")
RETENTION_DAYS = max(0, int(os.getenv("RETENTION_DAYS", "180")))
MENTION_BATCH_LIMIT = max(1, min(20, int(os.getenv("MENTION_BATCH_LIMIT", "5"))))
MAX_REPLY_CHARS = 4500
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
EXPORT_TTL = max(60, int(os.getenv("EXPORT_TTL", "600")))
EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "/tmp/line_bot_exports"))
export_tokens: dict[str, tuple[str, float]] = {}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("linebot")

if not CHANNEL_SECRET or not CHANNEL_ACCESS_TOKEN:
    raise RuntimeError("LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN must be set in .env")

configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)


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


def export_database_to_json(output_path: Path) -> None:
    """SQLite 데이터베이스의 테이블을 JSON으로 내보낸다."""
    connection = sqlite3.connect(str(DB_PATH), timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA busy_timeout=10000")
        table_names = [
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        ]
        result = {
            "exported_at": datetime.now(ZoneInfo(TIMEZONE_NAME)).isoformat(),
            "database": DB_PATH.name,
            "tables": {},
        }
        for table_name in table_names:
            # Table names come from sqlite_master and are quoted as identifiers.
            escaped_name = table_name.replace('"', '""')
            rows = connection.execute(f'SELECT * FROM "{escaped_name}"').fetchall()
            result["tables"][table_name] = [dict(row) for row in rows]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=2, default=str)
    finally:
        connection.close()


async def create_database_export() -> str:
    cleanup_expired_exports()
    if not PUBLIC_BASE_URL:
        raise RuntimeError("PUBLIC_BASE_URL 환경변수가 설정되지 않았습니다.")
    token = secrets.token_urlsafe(32)
    file_path = EXPORT_DIR / f"database_{token}.json"
    try:
        await asyncio.to_thread(export_database_to_json, file_path)
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


def mention_targets(message) -> tuple[list[str], int]:
    """Extract IDs only from LINE mention metadata; unresolved mentions aren't guessed."""
    mention = field(message, "mention")
    mentionees = field(mention, "mentionees", default=[]) or []
    targets: list[str] = []
    unresolved = 0
    for person in mentionees:
        kind = field(person, "type")
        if kind != "user":
            unresolved += 1  # e.g. @all, not a single user
            continue
        uid = field(person, "user_id", "userId")
        if uid:
            targets.append(str(uid))
        else:
            unresolved += 1
    return list(dict.fromkeys(targets)), unresolved


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
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT UNIQUE,
  user_id TEXT,
  chat_id TEXT NOT NULL,
  message_type TEXT NOT NULL,
  message_text TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_user_time ON messages(user_id, created_at);
CREATE TABLE IF NOT EXISTS mentions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT,
  chat_id TEXT NOT NULL,
  sender_id TEXT NOT NULL,
  target_id TEXT,
  original_text TEXT NOT NULL,
  created_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK(status IN ('pending','processing','delivered','unresolved')),
  processing_token TEXT,
  processing_since TEXT,
  delivered_at TEXT
);
CREATE TABLE IF NOT EXISTS processed_events (
  event_id TEXT PRIMARY KEY,
  processed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bot_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""


@asynccontextmanager
async def db_connect():
    """Open one aiosqlite connection and always close it exactly once.

    Do not await aiosqlite.connect() and then enter that same connection with
    ``async with``: aiosqlite's connection context manager starts its worker
    thread on entry, so doing both raises ``threads can only be started once``.
    """
    db = await aiosqlite.connect(DB_PATH, timeout=10)
    try:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA busy_timeout=10000")
        await db.execute("PRAGMA foreign_keys=ON")
        yield db
    finally:
        await db.close()


async def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with db_connect() as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(SCHEMA)
        # Lightweight additive migration for databases created by earlier drafts.
        columns = {r[1] for r in await (await db.execute("PRAGMA table_info(users)")).fetchall()}
        if "display_name" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN display_name TEXT")
        if "display_name_updated_at" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN display_name_updated_at TEXT")

        mention_columns = {r[1] for r in await (await db.execute("PRAGMA table_info(mentions)")).fetchall()}
        if mention_columns and "status" not in mention_columns:
            # Migrate the earlier draft schema (delivered INTEGER) without losing pending records.
            await db.execute("ALTER TABLE mentions RENAME TO mentions_legacy")
            await db.execute("""CREATE TABLE mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                chat_id TEXT NOT NULL,
                sender_id TEXT NOT NULL,
                target_id TEXT,
                original_text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                  CHECK(status IN ('pending','processing','delivered','unresolved')),
                processing_token TEXT,
                processing_since TEXT,
                delivered_at TEXT
            )""")
            legacy_columns = {r[1] for r in await (await db.execute("PRAGMA table_info(mentions_legacy)")).fetchall()}
            delivered_expr = "CASE WHEN delivered=1 THEN 'delivered' ELSE 'pending' END" if "delivered" in legacy_columns else "'pending'"
            delivered_at_expr = "CASE WHEN delivered=1 THEN created_at ELSE NULL END" if "delivered" in legacy_columns else "NULL"
            await db.execute(f"""INSERT INTO mentions
                (id,event_id,chat_id,sender_id,target_id,original_text,created_at,status,delivered_at)
                SELECT id,event_id,chat_id,sender_id,target_id,original_text,created_at,
                       {delivered_expr}, {delivered_at_expr}
                FROM mentions_legacy""")
            await db.execute("DROP TABLE mentions_legacy")
        else:
            for name, declaration in (
                ("processing_token", "TEXT"),
                ("processing_since", "TEXT"),
                ("delivered_at", "TEXT"),
            ):
                if name not in mention_columns:
                    await db.execute(f"ALTER TABLE mentions ADD COLUMN {name} {declaration}")

        await db.execute("CREATE INDEX IF NOT EXISTS idx_mentions_pending ON mentions(chat_id, target_id, status, id)")
        await db.commit()


async def record_event(event_id, user_id, chat_id, message_type, message_text, targets, unresolved):
    now = iso_now()
    async with db_connect() as db:
        await db.execute("BEGIN IMMEDIATE")
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
                """INSERT INTO messages(event_id,user_id,chat_id,message_type,message_text,created_at)
                   VALUES(?,?,?,?,?,?)""",
                (event_id, user_id, chat_id, message_type, message_text, now),
            )
            if message_type == "text" and user_id and message_text:
                for target_id in targets:
                    if target_id == user_id:
                        continue
                    await db.execute(
                        """INSERT INTO mentions(event_id,chat_id,sender_id,target_id,original_text,created_at,status)
                           VALUES(?,?,?,?,?,?,'pending')""",
                        (event_id, chat_id, user_id, target_id, message_text, now),
                    )
                if unresolved:
                    await db.execute(
                        """INSERT INTO mentions(event_id,chat_id,sender_id,target_id,original_text,created_at,status)
                           VALUES(?,?,?,NULL,?,?,'unresolved')""",
                        (event_id, chat_id, user_id, message_text, now),
                    )
            await db.commit()
            return {"is_new_user": is_new, "created_at": now}
        except Exception:
            await db.rollback()
            raise


async def claim_pending_mentions(chat_id: str, target_id: str, limit: int):
    """Atomically reserve a batch so concurrent webhook handlers don't both send it."""
    token = os.urandom(16).hex()
    now = iso_now()
    stale_before = (utc_now() - timedelta(minutes=3)).isoformat()
    async with db_connect() as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            # Recover claims stranded by a crashed worker. This is at-least-once, not exactly-once.
            await db.execute(
                """UPDATE mentions SET status='pending', processing_token=NULL, processing_since=NULL
                   WHERE status='processing' AND processing_since < ?""",
                (stale_before,),
            )
            rows = await (await db.execute(
                """SELECT id,sender_id,original_text,created_at FROM mentions
                   WHERE chat_id=? AND target_id=? AND status='pending'
                   ORDER BY id LIMIT ?""",
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


async def statistics(chat_id: str | None = None):
    local_now = datetime.now(ZoneInfo(TIMEZONE_NAME))
    local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = local_start.astimezone(timezone.utc).isoformat()
    end_utc = (local_start + timedelta(days=1)).astimezone(timezone.utc).isoformat()
    async with db_connect() as db:
        where = " WHERE chat_id=?" if chat_id else ""
        args = (chat_id,) if chat_id else ()
        total = (await (await db.execute(f"SELECT COUNT(*) FROM messages{where}", args)).fetchone())[0]
        day_sql = f"SELECT COUNT(*) FROM messages{where}{' AND' if where else ' WHERE'} created_at >= ? AND created_at < ?"
        today_count = (await (await db.execute(day_sql, (*args, start_utc, end_utc))).fetchone())[0]
        user_sql = f"SELECT COUNT(DISTINCT user_id) FROM messages{where}{' AND' if where else ' WHERE'} user_id IS NOT NULL"
        user_count = (await (await db.execute(user_sql, args)).fetchone())[0]
        top_sql = f"""SELECT user_id, COUNT(*) AS count FROM messages{where}
                       {'AND' if where else 'WHERE'} user_id IS NOT NULL
                       GROUP BY user_id ORDER BY count DESC LIMIT 5"""
        top = await (await db.execute(top_sql, args)).fetchall()
        return {"total": total, "today": today_count, "users": user_count, "top": top}


async def format_stats(title, stats, source_type=None, chat_raw_id=None, api=None):
    lines = [
        f"📊 {title}",
        "━━━━━━━━━━━━━━",
        f"💬 누적 메시지  {stats['total']:,}개",
        f"☀️ 오늘 메시지  {stats['today']:,}개",
        f"👥 활동 사용자  {stats['users']:,}명",
        "",
        "🏆 메시지 TOP 5",
    ]
    if not stats["top"]:
        lines.append("아직 집계된 메시지가 없어요.")
    else:
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for i, row in enumerate(stats["top"]):
            user_id = row["user_id"]
            name = None
            if api is not None:
                name = await display_name_for(source_type or "user", chat_raw_id, user_id, api)
            lines.append(f"{medals[i]} {name or user_id}  ·  {row['count']:,}개")
    return "\n".join(lines)


async def display_name_for(source_type: str, chat_raw_id: str | None, user_id: str | None, api):
    """Best-effort name lookup. Profile endpoints may fail due to membership/permission rules."""
    if not user_id:
        return None
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
        log.debug("Profile lookup unavailable for %s: %s", user_id, exc)
    async with db_connect() as db:
        row = await (await db.execute("SELECT display_name FROM users WHERE user_id=?", (user_id,))).fetchone()
        return row["display_name"] if row and row["display_name"] else None


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
            log.exception("Scheduled cleanup failed")
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


app = FastAPI(title="LINE Community Bot", version="1.0.0", lifespan=lifespan)


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
    return FileResponse(file_path, media_type="application/json", filename="database_export.json")


@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature")
    if not signature:
        raise HTTPException(status_code=400, detail="Missing X-Line-Signature")
    body_bytes = await request.body()
    try:
        events = parser.parse(body_bytes.decode("utf-8"), signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception:
        log.exception("Webhook payload parsing failed")
        raise HTTPException(status_code=400, detail="Invalid webhook payload")

    api = request.app.state.line_api
    for event in events:
        if not isinstance(event, MessageEvent):
            continue
        source = event.source
        message = event.message
        user_id, chat_id, source_type = source_identity(source)
        if not chat_id:
            log.info("Skipping event without usable chat identity")
            continue
        message_type = field(message, "type", default="unknown")
        text = field(message, "text") if message_type == "text" else None
        event_id = field(event, "webhook_event_id", "webhookEventId")
        targets, unresolved = mention_targets(message) if message_type == "text" else ([], 0)
        try:
            result = await record_event(event_id, user_id, chat_id, message_type, text, targets, unresolved)
        except Exception:
            log.exception("Database event processing failed")
            # Non-2xx encourages LINE retry. Idempotency prevents duplicate DB rows on retry.
            raise HTTPException(status_code=500, detail="Event persistence failed")
        if result is None:
            continue
        if message_type != "text" or not text:
            continue

        stripped = text.strip()
        reply_parts: list[str] = []
        claim = None
        is_admin = bool(user_id and user_id in ADMIN_USER_IDS)

        # 명령어 처리: 답장은 아래 공통 처리 구간에서 한 번에 전송한다.
        if stripped == "!help":
            reply_parts.append(
                "📖 봇 명령어 안내\n"
                "━━━━━━━━━━━━━━\n"
                "📊 통계\n"
                "• !stats — 현재 대화방 및 전체 통계\n"
                "• !stats group — 현재 대화방 통계\n"
                "• !stats me — 내 활동 기록\n"
                "• !stats all — 전체 통계 (관리자 전용)\n\n"
                "📨 멘션 기록\n"
                "• !mentions — 확인하지 못한 멘션 기록\n\n"
                "🛠️ 관리 기능\n"
                "• !db — 데이터베이스 JSON 내보내기 (관리자 전용)\n\n"
                "명령어를 채팅방에 입력하면 사용할 수 있어요."
            )
        elif stripped in {"!stats", "!stats group"}:
            raw_chat_id = field(source, "group_id", "groupId", "room_id", "roomId")
            reply_parts.append(await format_stats(
                "현재 대화방 통계", await statistics(chat_id), source_type, raw_chat_id, api
            ))
            if stripped == "!stats":
                reply_parts.append(await format_stats("전체 통계", await statistics(), "user", None, api))
        elif stripped == "!stats all":
            if is_admin:
                reply_parts.append(await format_stats("전체 통계", await statistics(), "user", None, api))
            else:
                reply_parts.append("🔒 관리자 전용 명령어예요.\n이 기능은 관리자만 사용할 수 있어요.")
        elif stripped == "!stats me":
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
        elif stripped == "!mentions":
            if not user_id:
                reply_parts.append("사용자 정보를 확인할 수 없어 멘션 기록을 표시할 수 없어요.")
            else:
                token, rows = await claim_pending_mentions(chat_id, user_id, MENTION_BATCH_LIMIT)
                if rows:
                    lines = ["📨 확인하지 못한 멘션", "━━━━━━━━━━━━━━"]
                    raw_chat_id = field(source, "group_id", "groupId", "room_id", "roomId")
                    for row in rows:
                        sender_name = await display_name_for(source_type, raw_chat_id, row["sender_id"], api)
                        lines.append(
                            f"\n👤 보낸 사람: {sender_name or row['sender_id']}"
                            f"\n📝 메시지: {row['original_text']}"
                            f"\n🕒 시간: {format_datetime(row['created_at'])}"
                        )
                    reply_parts.append("\n".join(lines))
                    claim = (token, [r["id"] for r in rows])
                else:
                    reply_parts.append("📭 현재 대화방에 확인하지 못한 멘션이 없어요.")
        elif stripped == "!db":
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
                lines = ["📨 이전 멘션 기록", "━━━━━━━━━━━━━━"]
                for row in rows:
                    sender_name = await display_name_for(
                        source_type,
                        field(source, "group_id", "groupId", "room_id", "roomId"),
                        row["sender_id"], api,
                    )
                    lines.append(
                        f"\n👤 보낸 사람: {sender_name or row['sender_id']}"
                        f"\n📝 메시지: {row['original_text']}"
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
            log.exception("Reply failed for chat=%s event=%s", chat_id, event_id)
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
        raise HTTPException(status_code=403, detail="Forbidden")
    await cleanup_old_data()
    return {"status": "ok", "retention_days": RETENTION_DAYS}
