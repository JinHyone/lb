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

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    AsyncApiClient,
    AsyncMessagingApi,
    AsyncMessagingApiBlob,
    Configuration,
    ReplyMessageRequest,
    PushMessageRequest,
    TextMessage,
    ImageMessage,
    StickerMessage,
)
from linebot.v3.webhook import WebhookParser
from linebot.v3.webhooks import MessageEvent

load_dotenv()

import database
from commands import parse_command
from database import db_connect
from line_utils import (
    event_occurred_at, field, iso_now, mention_targets, source_identity, utc_now,
)
from services import (
    claim_pending_mentions, cleanup_old_data, finish_claim, low_activity_users,
    membership_history, record_event, record_membership_event, statistics,
    warning_count,
)



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
    if database.db_pool is None:
        raise RuntimeError("데이터베이스 풀이 아직 초기화되지 않았습니다.")
    async with database.db_pool.acquire() as conn:
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















async def init_db():
    """Initialize PostgreSQL and restore persisted activity settings."""
    global MALE_GROUP_EMOJIS, FEMALE_GROUP_EMOJIS
    global MALE_DAILY_MIN_MESSAGES, FEMALE_DAILY_MIN_MESSAGES

    try:
        loaded = await database.initialize_database()

        if "activity_male_emojis" in loaded:
            MALE_GROUP_EMOJIS = tuple(
                json.loads(loaded["activity_male_emojis"])
            )

        if "activity_female_emojis" in loaded:
            FEMALE_GROUP_EMOJIS = tuple(
                json.loads(loaded["activity_female_emojis"])
            )

        if set(MALE_GROUP_EMOJIS) & set(FEMALE_GROUP_EMOJIS):
            raise RuntimeError(
                "DB의 남성/여성 이모티콘 설정이 중복됩니다. "
                "!소통설정으로 수정해 주세요."
            )

        if "activity_male_min" in loaded:
            MALE_DAILY_MIN_MESSAGES = max(
                1, int(loaded["activity_male_min"])
            )

        if "activity_female_min" in loaded:
            FEMALE_DAILY_MIN_MESSAGES = max(
                1, int(loaded["activity_female_min"])
            )

        defaults = {
            "activity_male_emojis": json.dumps(
                list(MALE_GROUP_EMOJIS), ensure_ascii=False
            ),
            "activity_female_emojis": json.dumps(
                list(FEMALE_GROUP_EMOJIS), ensure_ascii=False
            ),
            "activity_male_min": str(MALE_DAILY_MIN_MESSAGES),
            "activity_female_min": str(FEMALE_DAILY_MIN_MESSAGES),
        }

        async with db_connect() as db:
            for key, value in defaults.items():
                await db.execute(
                    """
                    INSERT INTO bot_metadata(key, value)
                    VALUES(?, ?)
                    ON CONFLICT(key) DO NOTHING
                    """,
                    (key, value),
                )
            await db.commit()

    except Exception:
        await database.close_pool()
        raise

















async def format_stats(title, stats, source_type=None, chat_raw_id=None, api=None):
    lines = [
        f"📊 {title} · {stats['date']}",
        "━━━━━━━━━━━━━━",
        f"💬 누적 메시지  {stats['total']:,}개",
        f"📅 해당 날짜 메시지  {stats['day_count']:,}개",
        f"👥 해당 날짜 활동 사용자  {stats['users']:,}명",
        f"🎯 그룹별 하루 최소 소통량: 💫 {MALE_DAILY_MIN_MESSAGES:,}개 / 📡 {FEMALE_DAILY_MIN_MESSAGES:,}개",
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
    app.state.line_blob_api = AsyncMessagingApiBlob(client)

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
        await database.close_pool()



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
        stripped = text.strip() if text else ""
        reply_parts: list[str] = []
        claim = None
        is_admin = bool(user_id and user_id in ADMIN_USER_IDS)

        parsed = parse_command(stripped)
        command = parsed.name
        stats_day_offset = parsed.stats_day_offset
        low_activity_offset = parsed.low_activity_offset
        activity_settings_argument = parsed.activity_settings_argument
        keyword_settings_argument = parsed.keyword_settings_argument

        # 키워드 반응 설정의 대기 세션은 관리자/대화방 단위로 저장되어 재시작 후에도 이어진다.
        # 취소 명령은 대기 중에도 우선 처리한다.
        if is_admin and command == "keyword_settings" and keyword_settings_argument.casefold() == "취소":
            async with db_connect() as db:
                await db.execute("DELETE FROM keyword_setup_sessions WHERE chat_id=? AND admin_user_id=?", (chat_id, user_id))
                await db.execute("DELETE FROM keyword_setup_items WHERE chat_id=? AND admin_user_id=?", (chat_id, user_id))
                await db.commit()
            await api.reply_message(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text="키워드 설정을 취소했어요.")]))
            continue

        if is_admin:
            async with db_connect() as db:
                setup_session = await (await db.execute(
                    "SELECT keyword, expected_count FROM keyword_setup_sessions WHERE chat_id=? AND admin_user_id=?",
                    (chat_id, user_id),
                )).fetchone()
            if setup_session and not (command == "keyword_settings" and keyword_settings_argument):
                item_type = message_type
                item_values = {"text_content": None, "sticker_package_id": None, "sticker_id": None, "image_data": None, "content_type": None}
                if message_type == "text" and text and text.strip() and not text.strip().startswith("!"):
                    item_values["text_content"] = text.strip()
                elif message_type == "sticker":
                    item_values["sticker_package_id"] = str(field(message, "package_id", "packageId", default="") or "")
                    item_values["sticker_id"] = str(field(message, "sticker_id", "stickerId", default="") or "")
                    if not item_values["sticker_package_id"] or not item_values["sticker_id"]:
                        item_type = "unsupported"
                elif message_type == "image":
                    if not PUBLIC_BASE_URL:
                        item_type = "unsupported"
                    else:
                        try:
                            content = await request.app.state.line_blob_api.get_message_content(str(line_message_id))
                            item_values["image_data"] = bytes(content)
                            item_values["content_type"] = "image/jpeg"
                        except Exception:
                            log.exception("키워드 설정용 사진을 가져오지 못했습니다.")
                            item_type = "unsupported"
                else:
                    item_type = "unsupported"

                if item_type != "unsupported":
                    async with db_connect() as db:
                        count_row = await (await db.execute(
                            "SELECT COUNT(*) AS n FROM keyword_setup_items WHERE chat_id=? AND admin_user_id=?",
                            (chat_id, user_id),
                        )).fetchone()
                        position = int(count_row["n"]) + 1
                        await db.execute(
                            """INSERT INTO keyword_setup_items
                               (chat_id,admin_user_id,position,item_type,text_content,sticker_package_id,sticker_id,image_data,content_type)
                               VALUES(?,?,?,?,?,?,?,?,?)""",
                            (chat_id, user_id, position, item_type, item_values["text_content"],
                             item_values["sticker_package_id"], item_values["sticker_id"],
                             item_values["image_data"], item_values["content_type"]),
                        )
                        count_row = await (await db.execute(
                            "SELECT COUNT(*) AS n FROM keyword_setup_items WHERE chat_id=? AND admin_user_id=?",
                            (chat_id, user_id),
                        )).fetchone()
                        collected = int(count_row["n"])
                        expected = int(setup_session["expected_count"])
                        keyword = setup_session["keyword"]
                        if collected >= expected:
                            await db.execute("DELETE FROM keyword_reaction_items WHERE chat_id=? AND keyword=?", (chat_id, keyword))
                            await db.execute(
                                "INSERT INTO keyword_reactions(chat_id,keyword,updated_at) VALUES(?,?,?) ON CONFLICT(chat_id,keyword) DO UPDATE SET updated_at=excluded.updated_at",
                                (chat_id, keyword, iso_now()),
                            )
                            staged = await (await db.execute(
                                "SELECT * FROM keyword_setup_items WHERE chat_id=? AND admin_user_id=? ORDER BY position",
                                (chat_id, user_id),
                            )).fetchall()
                            for staged_item in staged:
                                media_key = secrets.token_urlsafe(24) if staged_item["item_type"] == "image" else None
                                await db.execute(
                                    """INSERT INTO keyword_reaction_items
                                       (media_key,chat_id,keyword,position,item_type,text_content,sticker_package_id,sticker_id,image_data,content_type)
                                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                                    (media_key, chat_id, keyword, staged_item["position"], staged_item["item_type"],
                                     staged_item["text_content"], staged_item["sticker_package_id"], staged_item["sticker_id"],
                                     staged_item["image_data"], staged_item["content_type"]),
                                )
                            await db.execute("DELETE FROM keyword_setup_items WHERE chat_id=? AND admin_user_id=?", (chat_id, user_id))
                            await db.execute("DELETE FROM keyword_setup_sessions WHERE chat_id=? AND admin_user_id=?", (chat_id, user_id))
                        await db.commit()
                    if collected >= expected:
                        response_text = f"✅ 키워드 '{keyword}'의 반응 메시지 {expected}개를 저장했어요."
                    else:
                        response_text = f"📥 {keyword} 반응 메시지 {collected}/{expected}개 저장 완료예요. 다음 메시지를 보내 주세요."
                else:
                    response_text = "⚠️ 텍스트, 사진 또는 LINE 스티커만 설정할 수 있어요. 이 메시지는 개수에 포함하지 않았어요."
                    if message_type == "image" and not PUBLIC_BASE_URL:
                        response_text += " 사진 저장에는 공개 HTTPS 주소를 가리키는 PUBLIC_BASE_URL 환경변수가 필요해요."
                await api.reply_message(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=response_text)]))
                continue

        if message_type != "text" or not text:
            continue

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
                "• !소통설정 남성이모티콘 🔭,💫 — 남성 이모티콘 그룹 변경\n"
                "• !소통설정 여성이모티콘 🛰️,📡 — 여성 이모티콘 그룹 변경\n"
                "• !키워드 설정 안녕 3 — 키워드 반응 3개 설정 (관리자 전용)\n"
                "• !키워드 목록 — 키워드 목록 보기 (관리자 전용)\n"
                "• !키워드 삭제 안녕 — 키워드 삭제 (관리자 전용)\n"
                f"• 현재 기준 — {', '.join(MALE_GROUP_EMOJIS)} 남성 {MALE_DAILY_MIN_MESSAGES}개 / {', '.join(FEMALE_GROUP_EMOJIS)} 여성 {FEMALE_DAILY_MIN_MESSAGES}개\n\n"
                "📨 멘션·답장 알림\n"
                "• 나를 멘션하거나 내 메시지에 답장하면, 내가 다음에 일반 채팅을 입력할 때 알림 내용이 자동으로 표시돼요.\n"
                "• !멘션 — 자동 표시를 기다리지 않고 새 멘션·답장·전체 멘션 기록 확인\n"
                "※ 명령어(!로 시작하는 메시지)는 자동 알림 표시를 실행하지 않아요.\n\n"
                "👥 입퇴장 기록 (관리자 전용)\n"
                "• !입퇴장 — 현재 대화방의 최근 입퇴장 기록\n"
                "• !입퇴장 전체 — 현재 대화방의 최근 입퇴장 기록 목록\n"
                "• !입퇴장 사용자ID 또는 !입퇴장 @사용자 — 해당 사용자의 최초 입장 시각, 마지막 퇴장 시각 및 기록 확인\n"
                "※ 퇴장 시에만 대화방에 알림을 보내며, 입장 알림은 보내지 않아요."
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
                    f"💫 남성 이모티콘: {', '.join(MALE_GROUP_EMOJIS)}\n"
                    f"남성 하루 기준: {MALE_DAILY_MIN_MESSAGES}개\n\n"
                    f"📡 여성 이모티콘: {', '.join(FEMALE_GROUP_EMOJIS)}\n"
                    f"여성 하루 기준: {FEMALE_DAILY_MIN_MESSAGES}개\n\n"
                    "변경 명령어:\n"
                    "!소통설정 남성기준 90\n!소통설정 여성기준 70\n"
                    "!소통설정 남성이모티콘 💫,🔭\n!소통설정 여성이모티콘 🛰️,📡"
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
        elif command == "keyword_settings":
            if not is_admin:
                reply_parts.append("🔒 키워드 설정은 관리자만 사용할 수 있어요.")
            else:
                parts = keyword_settings_argument.split(maxsplit=1)
                if not parts:
                    reply_parts.append("사용법:\n!키워드 설정 안녕 3\n!키워드 목록\n!키워드 삭제 안녕\n!키워드 취소")
                elif parts[0] == "설정" and len(parts) == 2 and len(parts[1].rsplit(maxsplit=1)) == 2:
                    keyword, count_text = parts[1].rsplit(maxsplit=1)
                    keyword = keyword.strip()
                    try:
                        expected = int(count_text)
                        if not keyword or keyword.startswith("!") or len(keyword) > 100:
                            raise ValueError("키워드는 1~100자이며 !로 시작할 수 없어요.")
                        if not 1 <= expected <= 5:
                            raise ValueError("반응 개수는 1~5 사이여야 해요.")
                        async with db_connect() as db:
                            await db.execute("DELETE FROM keyword_setup_sessions WHERE chat_id=? AND admin_user_id=?", (chat_id, user_id))
                            await db.execute("DELETE FROM keyword_setup_items WHERE chat_id=? AND admin_user_id=?", (chat_id, user_id))
                            await db.execute("INSERT INTO keyword_setup_sessions(chat_id,admin_user_id,keyword,expected_count,created_at) VALUES(?,?,?,?,?) ON CONFLICT(chat_id,admin_user_id) DO UPDATE SET keyword=excluded.keyword, expected_count=excluded.expected_count, created_at=excluded.created_at", (chat_id, user_id, keyword, expected, iso_now()))
                            await db.commit()
                        reply_parts.append(f"🛠️ 키워드 '{keyword}' 설정을 시작해요.\n반응 메시지 {expected}개를 차례대로 보내 주세요. 텍스트·사진·LINE 스티커를 지원해요.\n설정 취소: !키워드 취소")
                    except ValueError as exc:
                        reply_parts.append(f"❌ {exc}")
                elif parts[0] == "삭제" and len(parts) == 2:
                    keyword = parts[1].strip()
                    async with db_connect() as db:
                        await db.execute("DELETE FROM keyword_reaction_items WHERE chat_id=? AND keyword=?", (chat_id, keyword))
                        deleted = await db.execute("DELETE FROM keyword_reactions WHERE chat_id=? AND keyword=?", (chat_id, keyword))
                        await db.commit()
                    reply_parts.append(f"🗑️ 키워드 '{keyword}' 설정을 삭제했어요." if deleted.rowcount else f"키워드 '{keyword}' 설정을 찾지 못했어요.")
                elif parts[0] == "목록" and len(parts) == 1:
                    async with db_connect() as db:
                        rows = await (await db.execute("SELECT r.keyword, COUNT(i.id) AS item_count FROM keyword_reactions r LEFT JOIN keyword_reaction_items i ON i.chat_id=r.chat_id AND i.keyword=r.keyword WHERE r.chat_id=? GROUP BY r.keyword ORDER BY r.keyword", (chat_id,))).fetchall()
                    reply_parts.append("📚 등록된 키워드\n" + ("\n".join(f"• {row['keyword']} ({row['item_count']}개)" for row in rows) if rows else "등록된 키워드가 없어요."))
                else:
                    reply_parts.append("❌ 사용법: !키워드 설정 (키워드) (1~5) / !키워드 목록 / !키워드 삭제 (키워드) / !키워드 취소")
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
                    f"기준: {', '.join(MALE_GROUP_EMOJIS)} 남성 {MALE_DAILY_MIN_MESSAGES:,}개 / {', '.join(FEMALE_GROUP_EMOJIS)} 여성 {FEMALE_DAILY_MIN_MESSAGES:,}개 미만", ""
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

        if not stripped.startswith("!"):
            async with db_connect() as db:
                reaction = await (await db.execute(
                    "SELECT keyword FROM keyword_reactions WHERE chat_id=? AND keyword=?", (chat_id, stripped)
                )).fetchone()
                reaction_items = []
                if reaction:
                    reaction_items = await (await db.execute(
                        "SELECT * FROM keyword_reaction_items WHERE chat_id=? AND keyword=? ORDER BY position LIMIT 5",
                        (chat_id, stripped),
                    )).fetchall()
            if reaction_items:
                outgoing = []
                for item in reaction_items:
                    if item["item_type"] == "text":
                        outgoing.append(TextMessage(text=item["text_content"] or ""))
                    elif item["item_type"] == "sticker":
                        outgoing.append(StickerMessage(package_id=str(item["sticker_package_id"]), sticker_id=str(item["sticker_id"])))
                    elif item["item_type"] == "image":
                        if not PUBLIC_BASE_URL or not item["media_key"]:
                            continue
                        image_url = f"{PUBLIC_BASE_URL}/keyword-media/{item['media_key']}"
                        outgoing.append(ImageMessage(original_content_url=image_url, preview_image_url=image_url))
                if outgoing:
                    await api.reply_message(ReplyMessageRequest(reply_token=event.reply_token, messages=outgoing[:5]))
                    continue

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


@app.get("/keyword-media/{media_key}")
async def keyword_media(media_key: str):
    """Serve keyword-response images to LINE through a public HTTPS URL."""
    async with db_connect() as db:
        row = await (await db.execute(
            "SELECT image_data, content_type FROM keyword_reaction_items WHERE media_key=?", (media_key,)
        )).fetchone()
    if not row or not row["image_data"]:
        raise HTTPException(status_code=404, detail="이미지를 찾을 수 없습니다.")
    return Response(content=bytes(row["image_data"]), media_type=row["content_type"] or "image/jpeg", headers={"Cache-Control": "public, max-age=86400"})


@app.post("/admin/cleanup")
async def admin_cleanup(request: Request):
    """Manual retention cleanup; requires X-Admin-Token matching ADMIN_API_TOKEN."""
    expected = os.getenv("ADMIN_API_TOKEN", "")
    supplied = request.headers.get("X-Admin-Token", "")
    if not expected or not supplied or not __import__("hmac").compare_digest(expected, supplied):
        raise HTTPException(status_code=403, detail="관리자 인증에 실패했습니다.")
    await cleanup_old_data()
    return {"status": "ok", "retention_days": RETENTION_DAYS}
