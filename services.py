from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .database import db_connect
from .line_utils import iso_now, utc_now

TIMEZONE_NAME = os.getenv("TIMEZONE", "Asia/Seoul")
RETENTION_DAYS = max(0, int(os.getenv("RETENTION_DAYS", "180")))

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


async def record_membership_event(event_id: str | None, chat_id: str, user_id: str,
                                  event_type: str, occurred_at: str) -> bool:
    """Insert a membership event once, atomically under concurrent webhook retries."""
    async with db_connect() as db:
        cursor = await db.execute(
            """INSERT INTO membership_events(
                   event_id,chat_id,user_id,event_type,occurred_at
               ) VALUES(?,?,?,?,?)
               ON CONFLICT (event_id,user_id,event_type)
               WHERE event_id IS NOT NULL DO NOTHING
               RETURNING id""",
            (event_id, chat_id, user_id, event_type, occurred_at),
        )
        await db.commit()
    return cursor.rowcount > 0


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

