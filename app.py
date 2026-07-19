from __future__ import annotations

import os
import random
import re
import sqlite3
import string
from collections import Counter
from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path

import psycopg
import yt_dlp
from flask import Flask, jsonify, redirect, render_template, request, url_for

app = Flask(__name__)

ROOMS: dict[str, dict] = {}
ROOM_MAX_ACTIVE = int(os.environ.get("ROOM_MAX_ACTIVE", "300"))
ROOM_TTL_SECONDS = int(os.environ.get("ROOM_TTL_SECONDS", str(6 * 60 * 60)))
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
SQLITE_DB_PATH = Path(
    os.environ.get(
        "SQLITE_DB_PATH",
        Path(__file__).resolve().parent.parent / "youke_karaoke.db",
    )
)
AUTO_REFRESH_SEARCH_LIMIT = int(os.environ.get("AUTO_REFRESH_SEARCH_LIMIT", "6"))
AUTO_REFRESH_SAVE_LIMIT = int(os.environ.get("AUTO_REFRESH_SAVE_LIMIT", "12"))
AUTO_REFRESH_PATTERNS = [
    "{query} karaoke",
    "{query} official karaoke",
    "{query} official mv",
    "{query} official video",
]

STYLE_PRESETS = [
    {"id": "pop", "name": "Pop", "keywords": ["pop", "เพลงฮิตไทย", "เพลงไทย"]},
    {"id": "rock", "name": "Rock", "keywords": ["rock", "ร็อค", "เพลงร็อค"]},
    {"id": "lukthung", "name": "ลูกทุ่ง", "keywords": ["ลูกทุ่ง"]},
    {"id": "morlam", "name": "หมอลำ", "keywords": ["หมอลำ"]},
    {"id": "dance", "name": "Dance", "keywords": ["dance", "แดนซ์"]},
    {"id": "indie", "name": "Indie", "keywords": ["indie", "อินดี้"]},
    {"id": "ost", "name": "OST", "keywords": ["ost", "ละคร"]},
    {"id": "90s", "name": "90s", "keywords": ["90", "ยุค 90"]},
    {"id": "2000s", "name": "2000s", "keywords": ["2000", "ยุค 2000"]},
    {"id": "duet", "name": "เพลงคู่", "keywords": ["เพลงคู่", "feat", " x "]},
    {"id": "heartbreak", "name": "อกหัก", "keywords": ["อกหัก"]},
    {"id": "party", "name": "งานเลี้ยง", "keywords": ["งานเลี้ยง", "สนุก"]},
]

STYLE_TITLE_BLOCKLIST = [
    r"\bofficial\s*mv\b",
    r"\bofficial\s*video\b",
    r"\blive\s*(ver\.?|version)?\b",
    r"\bliveversion\b",
    r"\blivehouse\b",
    r"\bconcert\b",
    r"\breaction\b",
    r"\breview\b",
    r"\bteaser\b",
    r"\btrailer\b",
    r"\bcover\b",
    r"\bsetup\b",
    r"\bconfiguration\b",
    r"\btutorial\b",
    r"\bhow\s*to\b",
    r"\bsteps?\s+for\b",
    r"\bfacility\b",
    r"\bmotorhome\b",
    r"\bspeaker\b",
    r"\bsound\s*system\b",
    r"\beco\s*houses?\b",
    r"\bextreme\s*karaoke\b",
    r"\bmidi\s*emk\b",
    r"\bhardlock\b",
    r"\bsoftware\b",
    r"\bprogram\b",
    r"\binterview\b",
    r"\bdocumentary\b",
    r"\bepisode\b",
    r"\bep\.?\s*\d+\b",
    r"\bopening\s*&\s*closing\b",
    r"\bopening\s*/\s*closing\b",
    r"\bopening\s+to\s+vcd\b",
    r"\bopening\s+to\b.*\bvcd\b",
    r"\blongplay\b",
    r"\bmedley\b",
    r"\bnonstop\b",
    r"\bplaylist\b",
    r"\bfull\s*album\b",
]

STYLE_TITLE_TEXT_BLOCKLIST = [
    "รวมเพลง",
    "รวมฮิต",
    "ต่อเนื่อง",
    "เต็มอัลบั้ม",
    "เบื้องหลัง",
    "มีเสียงร้อง",
    "วิธี",
    "สอน",
    "รีวิว",
    "เครื่องเสียง",
    "ระบบเสียง",
    "เพิ่มเพลง",
    "ตัดเสียงร้อง",
]

STYLE_KARAOKE_SIGNALS = [
    "karaoke",
    "คาราโอเกะ",
    "instrumental",
    "minus one",
    "kamioke",
    "ร้องตาม",
    "ร้องได้",
]

LABEL_PRESETS = [
    {
        "id": "genie",
        "name": "GENIE",
        "logo": "/static/label_logos/genie.png",
        "patterns": ["%genie%"],
    },
    {
        "id": "gmm",
        "name": "GMM",
        "logo": "/static/label_logos/gmm.png",
        "patterns": ["GMM Karaoke%", "%GMM Grammy%"],
    },
    {
        "id": "grammy_gold",
        "name": "GRAMMY GOLD",
        "logo": "/static/label_logos/grammy_gold.jpg",
        "patterns": ["%Grammy Gold%"],
    },
    {
        "id": "kamikaze",
        "name": "KAMIKAZE",
        "logo": "/static/label_logos/kamikaze.png",
        "patterns": ["%KAMIKAZE%", "%kamikaze%", "%welovekamikaze%"],
    },
    {
        "id": "rs",
        "name": "RS",
        "logo": "/static/label_logos/rs.png",
        "patterns": ["RS KARAOKE%", "RS MV%", "%คาราโอเกะ rs%"],
    },
    {
        "id": "r_siam",
        "name": "R-SIAM",
        "logo": "/static/label_logos/r_siam.jpg",
        "patterns": ["%RSiam%", "%R Siam%"],
    },
]


def generate_code(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def extract_video_id(value: str | None) -> str | None:
    if not value:
        return None

    value = value.strip()

    if re.fullmatch(r"[A-Za-z0-9_-]{11}", value):
        return value

    patterns = [
        r"(?:v=)([A-Za-z0-9_-]{11})",
        r"(?:youtu\.be/)([A-Za-z0-9_-]{11})",
        r"(?:embed/)([A-Za-z0-9_-]{11})",
        r"(?:shorts/)([A-Za-z0-9_-]{11})",
    ]

    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            return match.group(1)

    return None


def get_thumbnail(video_id: str) -> str:
    return f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"


def get_room(code: str) -> dict | None:
    room = ROOMS.get(code)
    if room:
        room["last_seen_at"] = now_ms()
    return room


def cleanup_inactive_rooms() -> int:
    cutoff = now_ms() - (ROOM_TTL_SECONDS * 1000)
    stale_codes = [
        code for code, room in ROOMS.items()
        if int(room.get("last_seen_at", room.get("created_at", 0))) < cutoff
    ]
    for code in stale_codes:
        ROOMS.pop(code, None)
    return len(stale_codes)


def active_room_count() -> int:
    cleanup_inactive_rooms()
    return len(ROOMS)


def room_capacity_context() -> dict:
    active_count = active_room_count()
    return {
        "active_room_count": active_count,
        "room_max_active": ROOM_MAX_ACTIVE,
        "room_capacity_full": active_count >= ROOM_MAX_ACTIVE,
    }


def using_postgres() -> bool:
    return bool(DATABASE_URL)


def db_ready() -> bool:
    return using_postgres() or SQLITE_DB_PATH.exists()


def sql_placeholder() -> str:
    return "%s" if using_postgres() else "?"


def like_operator() -> str:
    return "ILIKE" if using_postgres() else "LIKE"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


REACTION_MAX_CHARS = 80
REACTION_MAX_WORDS = 10
REACTION_COOLDOWN_MS = 1200
REACTION_ROOM_WINDOW_MS = 10000
REACTION_ROOM_LIMIT = 20
REACTION_URL_RE = re.compile(
    r"(https?://|www\.|(?:^|\s)[\w.-]+\.(?:com|net|org|co|io|app|th)(?:\s|/|$))",
    re.IGNORECASE,
)
REACTION_BLOCKED_RE = re.compile(
    "|".join(
        [
            r"\bf+u+c+k+\b",
            r"\bs+h+i+t+\b",
            r"\bb+i+t+c+h+\b",
            r"\ba+s+s+h+o+l+e+\b",
            r"\bp+o+r+n+\b",
            r"\bx+x+x+\b",
            "\u0e40\u0e2b\u0e35\u0e49\u0e22",
            "\u0e04\u0e27\u0e22",
            "\u0e2a\u0e31\u0e2a",
            "\u0e2a\u0e31\u0e14",
            "\u0e2b\u0e35",
            "\u0e40\u0e22\u0e47\u0e14",
        ]
    ),
    re.IGNORECASE,
)


def normalize_reaction_text(raw_message: str) -> str:
    message = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", raw_message or "")
    message = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", message)
    return re.sub(r"\s+", " ", message.strip())


def reject_reaction(error: str, code: str, status: int = 400):
    return jsonify({"error": error, "code": code}), status


def reaction_word_count(message: str) -> int:
    return len(message.split())


def room_reaction_rate_limited(room: dict, current_ms: int) -> bool:
    cutoff = current_ms - REACTION_ROOM_WINDOW_MS
    recent = [
        reaction for reaction in room.get("reactions", [])
        if int(reaction.get("createdAt", 0)) >= cutoff
    ]
    return len(recent) >= REACTION_ROOM_LIMIT


@contextmanager
def db_connect():
    if using_postgres():
        conn = psycopg.connect(DATABASE_URL)
    else:
        conn = sqlite3.connect(SQLITE_DB_PATH)

    try:
        yield conn
    finally:
        conn.close()


def ensure_failed_videos_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS failed_videos (
            video_id TEXT PRIMARY KEY,
            reason TEXT,
            checked_at TEXT
        )
        """
    )
    conn.commit()


def ensure_song_request_stats_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS song_request_stats (
            video_id TEXT PRIMARY KEY,
            request_count INTEGER NOT NULL DEFAULT 0,
            last_requested_at TEXT
        )
        """
    )
    conn.commit()


def ensure_search_refresh_jobs_table(conn) -> None:
    cur = conn.cursor()
    id_type = "SERIAL PRIMARY KEY" if using_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS search_refresh_jobs (
            id {id_type},
            query TEXT NOT NULL,
            normalized_query TEXT NOT NULL,
            status TEXT NOT NULL,
            requested_by_ip TEXT,
            created_at TEXT,
            finished_at TEXT,
            found_count INTEGER NOT NULL DEFAULT 0,
            error TEXT
        )
        """
    )
    conn.commit()


def record_song_request(video_id: str) -> None:
    if not db_ready() or not video_id:
        return

    placeholder = sql_placeholder()
    now = utc_now_iso()

    with db_connect() as conn:
        ensure_song_request_stats_table(conn)
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO song_request_stats (video_id, request_count, last_requested_at)
            VALUES ({placeholder}, 1, {placeholder})
            ON CONFLICT(video_id) DO UPDATE SET
                request_count = song_request_stats.request_count + 1,
                last_requested_at = excluded.last_requested_at
            """,
            (video_id, now),
        )
        conn.commit()


def mark_video_failed(video_id: str, reason: str = "player_error") -> None:
    if not db_ready():
        return

    placeholder = sql_placeholder()

    with db_connect() as conn:
        ensure_failed_videos_table(conn)
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE karaoke_songs
            SET embed_ok = 0
            WHERE video_id = {placeholder}
            """,
            (video_id,),
        )
        cur.execute(
            f"""
            INSERT INTO failed_videos (video_id, reason, checked_at)
            VALUES ({placeholder}, {placeholder}, {placeholder})
            ON CONFLICT(video_id) DO UPDATE SET
                reason = excluded.reason,
                checked_at = excluded.checked_at
            """,
            (video_id, reason, utc_now_iso()),
        )
        conn.commit()


def advance_room_after_failed_video(room: dict, video_id: str) -> None:
    room["queue"] = [item for item in room["queue"] if item.get("videoId") != video_id]

    if room["current"] and room["current"].get("videoId") == video_id:
        if room["queue"]:
            room["current"] = room["queue"].pop(0)
            room["is_playing"] = True
        else:
            room["current"] = None
            room["is_playing"] = False
        room["player_nonce"] += 1


def get_song_by_video_id(video_id: str) -> dict | None:
    if not db_ready():
        return None

    placeholder = sql_placeholder()

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                video_id,
                title,
                channel_name,
                thumbnail,
                score,
                embed_ok,
                source_name
            FROM karaoke_songs
            WHERE video_id = {placeholder}
              AND embed_ok = 1
              AND video_id NOT IN (SELECT video_id FROM failed_videos)
            LIMIT 1
            """,
            (video_id,),
        )
        row = cur.fetchone()

    if not row:
        return None

    return {
        "videoId": row[0],
        "title": row[1],
        "channelName": row[2] or "",
        "thumbnail": row[3] or get_thumbnail(row[0]),
        "score": row[4] or 0,
        "embedOk": row[5] or 0,
        "sourceName": row[6] or "",
    }


def search_songs(query: str, limit: int = 20) -> list[dict]:
    if not db_ready():
        return []

    q = (query or "").strip()
    if not q:
        return []

    terms = [t.strip() for t in q.split() if t.strip()]
    if not terms:
        return []

    where_parts: list[str] = [
        "embed_ok = 1",
        "video_id NOT IN (SELECT video_id FROM failed_videos)",
    ]
    params: list = []
    placeholder = sql_placeholder()
    op = like_operator()

    for term in terms:
        where_parts.append(f"(title {op} {placeholder} OR channel_name {op} {placeholder})")
        params.extend([f"%{term}%", f"%{term}%"])

    full_q = q
    full_prefix = f"{q}%"
    full_any = f"%{q}%"

    ranking_sql = f"""
        CASE
            WHEN title {op} {placeholder} THEN 500
            WHEN title {op} {placeholder} THEN 350
            WHEN title {op} {placeholder} THEN 220
            WHEN channel_name {op} {placeholder} THEN 120
            ELSE 0
        END
        + COALESCE(score, 0)
    """
    ranking_params = [full_q, full_prefix, full_any, full_any]

    sql = f"""
        SELECT
            video_id,
            title,
            channel_name,
            thumbnail,
            score,
            embed_ok,
            {ranking_sql} AS final_rank
        FROM karaoke_songs
        WHERE {" AND ".join(where_parts)}
        ORDER BY final_rank DESC, title ASC
        LIMIT {placeholder}
    """

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, ranking_params + params + [limit])
        rows = cur.fetchall()

    return [
        {
            "videoId": r[0],
            "title": r[1],
            "channelName": r[2] or "",
            "thumbnail": r[3] or get_thumbnail(r[0]),
            "score": r[4] or 0,
        }
        for r in rows
    ]


def normalize_refresh_query(query: str) -> str:
    return re.sub(r"\s+", " ", (query or "").strip().lower())[:120]


def create_search_refresh_job(query: str, requested_by_ip: str | None) -> int | None:
    if not db_ready():
        return None

    placeholder = sql_placeholder()
    normalized = normalize_refresh_query(query)

    with db_connect() as conn:
        ensure_search_refresh_jobs_table(conn)
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO search_refresh_jobs
                (query, normalized_query, status, requested_by_ip, created_at)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
            """,
            (query, normalized, "running", requested_by_ip, utc_now_iso()),
        )
        job_id = cur.lastrowid if not using_postgres() else None
        if using_postgres():
            cur.execute("SELECT LASTVAL()")
            job_id = int(cur.fetchone()[0])
        conn.commit()
        return job_id


def finish_search_refresh_job(job_id: int | None, status: str, found_count: int = 0, error: str | None = None) -> None:
    if not db_ready() or job_id is None:
        return

    placeholder = sql_placeholder()
    with db_connect() as conn:
        ensure_search_refresh_jobs_table(conn)
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE search_refresh_jobs
            SET status = {placeholder},
                finished_at = {placeholder},
                found_count = {placeholder},
                error = {placeholder}
            WHERE id = {placeholder}
            """,
            (status, utc_now_iso(), found_count, error, job_id),
        )
        conn.commit()


def save_discovered_song(video_id: str, title: str, channel_name: str, query: str) -> bool:
    video_id = extract_video_id(video_id)
    title = (title or "").strip()
    if not video_id or not title:
        return False

    placeholder = sql_placeholder()
    source_name = f"Auto Refresh - {normalize_refresh_query(query)[:80]}"

    if using_postgres():
        insert_sql = f"""
            INSERT INTO karaoke_songs
                (video_id, title, channel_name, thumbnail, duration, country, language, score, embed_ok, source_name, created_at)
            VALUES
                ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
            ON CONFLICT (video_id) DO NOTHING
        """
    else:
        insert_sql = f"""
            INSERT OR IGNORE INTO karaoke_songs
                (video_id, title, channel_name, thumbnail, duration, country, language, score, embed_ok, source_name, created_at)
            VALUES
                ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
        """

    with db_connect() as conn:
        ensure_failed_videos_table(conn)
        cur = conn.cursor()
        cur.execute(
            insert_sql,
            (
                video_id,
                title[:500],
                (channel_name or "YouTube Auto Refresh")[:255],
                get_thumbnail(video_id),
                None,
                "TH",
                "th",
                15,
                1,
                source_name,
                utc_now_iso(),
            ),
        )
        changed = cur.rowcount > 0
        conn.commit()
        return changed


def discover_and_save_songs_for_query(query: str) -> int:
    q = normalize_refresh_query(query)
    if not q:
        return 0

    saved = 0
    seen_video_ids: set[str] = set()
    ydl_opts = {
        "extract_flat": "in_playlist",
        "ignoreerrors": True,
        "nocheckcertificate": True,
        "noplaylist": True,
        "quiet": True,
        "skip_download": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for pattern in AUTO_REFRESH_PATTERNS:
            if saved >= AUTO_REFRESH_SAVE_LIMIT:
                break

            search_text = pattern.format(query=q)
            info = ydl.extract_info(f"ytsearch{AUTO_REFRESH_SEARCH_LIMIT}:{search_text}", download=False)
            entries = (info or {}).get("entries") or []

            for entry in entries:
                if saved >= AUTO_REFRESH_SAVE_LIMIT:
                    break
                if not entry:
                    continue

                video_id = extract_video_id(entry.get("id") or entry.get("url"))
                if not video_id or video_id in seen_video_ids:
                    continue

                seen_video_ids.add(video_id)
                title = entry.get("title") or ""
                channel_name = entry.get("uploader") or entry.get("channel") or "YouTube Auto Refresh"
                if save_discovered_song(video_id, title, channel_name, q):
                    saved += 1

    return saved


def normalize_locale_token(value: str | None) -> str:
    return re.sub(r'[^a-z0-9]+', '', (value or '').casefold())


def locale_preferences(country: str | None = '', language: str | None = '', locale: str | None = '') -> tuple[set[str], set[str]]:
    raw_values = [country or '', language or '', locale or '']
    locale_text = ' '.join(raw_values).casefold()
    country_tokens = {normalize_locale_token(value) for value in raw_values if value}
    language_tokens = {normalize_locale_token(value) for value in raw_values if value}
    aliases = {
        'th': ({'th', 'tha', 'thai', 'thailand'}, {'th', 'tha', 'thai'}),
        'en': ({'us', 'usa', 'unitedstates', 'gb', 'uk', 'au', 'ca'}, {'en', 'eng', 'english'}),
        'ja': ({'jp', 'jpn', 'japan'}, {'ja', 'jpn', 'japanese'}),
        'ko': ({'kr', 'kor', 'korea', 'southkorea'}, {'ko', 'kor', 'korean'}),
        'zh': ({'cn', 'chn', 'china', 'tw', 'taiwan', 'hk', 'hongkong'}, {'zh', 'zho', 'chi', 'chinese', 'mandarin', 'cantonese'}),
    }
    for prefix, (country_aliases, language_aliases) in aliases.items():
        if any(token.startswith(prefix) for token in country_tokens | language_tokens) or prefix in locale_text:
            country_tokens.update(country_aliases)
            language_tokens.update(language_aliases)
    country_tokens.discard('')
    language_tokens.discard('')
    return country_tokens, language_tokens


def song_locale_score(song: dict, preferred_countries: set[str], preferred_languages: set[str]) -> int:
    country = normalize_locale_token(song.get('country') or '')
    language = normalize_locale_token(song.get('language') or '')
    score = 0
    if country and country in preferred_countries:
        score += 260
    if language and language in preferred_languages:
        score += 220
    return score


def get_song_locale_map(video_ids: list[str]) -> dict[str, dict]:
    if not db_ready() or not video_ids:
        return {}
    placeholder = sql_placeholder()
    placeholders = ', '.join([placeholder for _ in video_ids])
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT video_id, country, language
            FROM karaoke_songs
            WHERE video_id IN ({placeholders})
            """,
            video_ids,
        )
        rows = cur.fetchall()
    return {row[0]: {'country': row[1] or '', 'language': row[2] or ''} for row in rows}


def get_recommended_songs(limit: int = 5, country: str = '', language: str = '', locale: str = '') -> list[dict]:
    if not db_ready():
        return []

    placeholder = sql_placeholder()
    op = like_operator()
    preferred_countries, preferred_languages = locale_preferences(country, language, locale)

    stats_sql = f"""
        SELECT
            s.video_id,
            s.title,
            s.channel_name,
            s.thumbnail,
            s.score,
            COALESCE(r.request_count, 0) AS app_requests
        FROM song_request_stats r
        JOIN karaoke_songs s ON s.video_id = r.video_id
        WHERE s.embed_ok = 1
          AND s.video_id NOT IN (SELECT video_id FROM failed_videos)
        ORDER BY app_requests DESC, r.last_requested_at DESC, s.title ASC
        LIMIT {placeholder}
    """

    results = []
    seen_video_ids = set()

    with db_connect() as conn:
        ensure_song_request_stats_table(conn)
        cur = conn.cursor()
        cur.execute(stats_sql, (limit,))
        rows = cur.fetchall()

        for row in rows:
            seen_video_ids.add(row[0])
            results.append(
                {
                    "videoId": row[0],
                    "title": row[1],
                    "channelName": row[2] or "",
                    "thumbnail": row[3] or get_thumbnail(row[0]),
                    "score": row[4] or 0,
                    "requestCount": row[5] or 0,
                    "country": "",
                    "language": "",
                }
            )

        if len(results) >= limit:
            return results[:limit]

        exclude_clause = ""
        exclude_params: list = []
        if seen_video_ids:
            exclude_clause = f"AND video_id NOT IN ({', '.join([placeholder for _ in seen_video_ids])})"
            exclude_params = list(seen_video_ids)

        fallback_limit = limit - len(results)
        trusted_patterns = [
            "GMM Karaoke%",
            "RS KARAOKE%",
            "%RSiam Official Karaoke%",
            "%KAMIKAZE%",
            "%welovekamikaze%",
        ]
        fallback_sql = f"""
            SELECT video_id, title, channel_name, thumbnail, score, country, language
            FROM karaoke_songs
            WHERE embed_ok = 1
              AND video_id NOT IN (SELECT video_id FROM failed_videos)
              AND COALESCE(channel_name, '') NOT {op} {placeholder}
              AND COALESCE(channel_name, '') NOT {op} {placeholder}
              {exclude_clause}
              AND ({' OR '.join([f'channel_name {op} {placeholder}' for _ in trusted_patterns])})
            ORDER BY COALESCE(score, 0) DESC, RANDOM()
            LIMIT {placeholder}
        """
        cur.execute(
            fallback_sql,
            ["Search V%", "Gen TH%"] + exclude_params + trusted_patterns + [fallback_limit],
        )
        fallback_rows = cur.fetchall()

    for row in fallback_rows:
        results.append(
            {
                "videoId": row[0],
                "title": row[1],
                "channelName": row[2] or "",
                "thumbnail": row[3] or get_thumbnail(row[0]),
                "score": row[4] or 0,
                "requestCount": 0,
                "country": row[5] or "",
                "language": row[6] or "",
            }
        )

    if preferred_countries or preferred_languages:
        locale_map = get_song_locale_map([song["videoId"] for song in results])
        for song in results:
            song.update(locale_map.get(song["videoId"], {}))
        results.sort(
            key=lambda song: (
                -song_locale_score(song, preferred_countries, preferred_languages),
                -int(song.get("requestCount") or 0),
                -float(song.get("score") or 0),
                song.get("title") or "",
            )
        )

    return results[:limit]


def clean_singer_candidate(value: str, query: str) -> str | None:
    value = re.split(r"[\[\(]", value or "", maxsplit=1)[0]
    value = re.sub(r"#\S+", " ", value)
    value = re.split(r"\b(?:feat\.?|ft\.?|featuring|with|x)\b", value, maxsplit=1, flags=re.IGNORECASE)[0]
    value = re.split(r"\s(?:ร่วมกับ|และ)\s", value, maxsplit=1)[0]
    value = re.sub(r"\b(?:live\s*ver\.?|version|ver\.?)\b.*$", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"(?:มีเสียงร้อง|ต้นฉบับ|official).*$", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\b(?:original|karaoke|kamioke|instrumental|lyrics?)\b.*$", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\b(?:kamikaze|gmm|grammy|rs|r-siam)\b$", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value or "").strip(" -|:/[]()")
    if not value:
        return None

    lowered = value.lower()
    noise_words = [
        "karaoke",
        "official",
        "audio",
        "lyrics",
        "lyric",
        "cover",
        "remix",
        "mv",
        "ost",
        "คาราโอเกะ",
        "เนื้อเพลง",
        "ร้องคาราโอเกะ",
        "official mv",
        "official audio",
        "original karaoke",
        "search",
        "playlist",
        "vcd",
        "midi",
        "version",
        "คาราโอเกะ",
        "เนื้อเพลง",
        "เพลงไทย",
        "เพลงลูกทุ่ง",
        "ร้องเพลง",
        "ดนตรี",
        "backing track",
        "female key",
        "male key",
        "all kamikaze",
        "directors cut",
        "eurovision",
        "doctor doctor",
        "welovekamikaze",
    ]
    if any(word in lowered for word in noise_words):
        return None

    if lowered in {"all", "various artists", "unknown"}:
        return None

    if len(value) < 2 or len(value) > 48:
        return None

    if query and query.lower() not in lowered:
        return None

    return value


def canonical_singer_key(value: str) -> str:
    key = (value or "").casefold()
    key = re.sub(r"[\s\-.|:/_()\[\],]+", "", key)
    return key


def extract_singer_candidates(title: str, channel_name: str, query: str) -> list[str]:
    candidates: list[str] = []
    text = title or ""

    marker_patterns = [
        r"ศิลปิน\s*[:：-]\s*([^|()\[\]\n]+)",
        r"artist\s*[:：-]\s*([^|()\[\]\n]+)",
        r"singer\s*[:：-]\s*([^|()\[\]\n]+)",
    ]
    for pattern in marker_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            cleaned = clean_singer_candidate(match.group(1), query)
            if cleaned:
                candidates.append(cleaned)

    pieces = re.split(r"\s[-–—|:：]\s|[|]", text)
    for piece in pieces[1:]:
        cleaned = clean_singer_candidate(piece, query)
        if cleaned:
            candidates.append(cleaned)

    cleaned_channel = clean_singer_candidate(channel_name or "", query)
    if cleaned_channel:
        candidates.append(cleaned_channel)

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = canonical_singer_key(candidate)
        if key not in seen:
            deduped.append(candidate)
            seen.add(key)
    return deduped


def search_singers(query: str, limit: int = 20) -> list[dict]:
    if not db_ready():
        return []

    q = (query or "").strip()
    if not q:
        return []

    placeholder = sql_placeholder()
    op = like_operator()
    sql = f"""
        SELECT title, channel_name
        FROM karaoke_songs
        WHERE embed_ok = 1
          AND video_id NOT IN (SELECT video_id FROM failed_videos)
          AND (title {op} {placeholder} OR channel_name {op} {placeholder})
        LIMIT {placeholder}
    """

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, (f"%{q}%", f"%{q}%", 500))
        rows = cur.fetchall()

    counts: dict[str, int] = {}
    display_names: dict[str, str] = {}
    for title, channel_name in rows:
        for candidate in extract_singer_candidates(title or "", channel_name or "", q):
            key = canonical_singer_key(candidate)
            counts[key] = counts.get(key, 0) + 1
            display_names.setdefault(key, candidate)

    ranked = sorted(counts.items(), key=lambda item: (-item[1], display_names[item[0]].lower()))
    return [
        {
            "name": display_names[key],
            "songCount": count,
        }
        for key, count in ranked[:limit]
    ]


def search_labels(query: str, limit: int = 20) -> list[dict]:
    if not db_ready():
        return []

    q = (query or "").strip()
    results: list[dict] = []
    presets = LABEL_PRESETS + [{"id": "other", "name": "Other", "logo": "", "patterns": []}]

    with db_connect() as conn:
        cur = conn.cursor()
        for preset in presets:
            if q and q.casefold() not in preset["name"].casefold():
                continue

            condition, params = label_filter_sql(preset["id"])
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM karaoke_songs
                WHERE embed_ok = 1
                  AND video_id NOT IN (SELECT video_id FROM failed_videos)
                  AND {condition}
                """,
                params,
            )
            count = int(cur.fetchone()[0])
            results.append(
                {
                    "id": preset["id"],
                    "name": preset["name"],
                    "logo": preset["logo"],
                    "songCount": count,
                }
            )

    return results[:limit]


def get_label_preset(label: str) -> dict | None:
    label_key = (label or "").strip().casefold()
    if label_key == "other":
        return {"id": "other", "name": "Other", "logo": "", "patterns": []}

    for preset in LABEL_PRESETS:
        if label_key in {preset["id"].casefold(), preset["name"].casefold()}:
            return preset
    return None


def label_filter_sql(label: str) -> tuple[str, list]:
    preset = get_label_preset(label)
    placeholder = sql_placeholder()
    op = like_operator()

    if not preset:
        return f"(source_name = {placeholder} OR channel_name = {placeholder})", [label, label]

    def pattern_condition() -> tuple[str, list]:
        parts: list[str] = []
        params: list = []
        for pattern in preset["patterns"]:
            parts.append(
                f"(COALESCE(source_name, '') {op} {placeholder} OR COALESCE(channel_name, '') {op} {placeholder})"
            )
            params.extend([pattern, pattern])
        trusted_source_only = (
            f"COALESCE(source_name, '') NOT {op} {placeholder} "
            f"AND COALESCE(source_name, '') NOT {op} {placeholder}"
        )
        params.extend(["Search V%", "Gen TH%"])
        return f"({' OR '.join(parts)}) AND {trusted_source_only}", params

    if preset["id"] != "other":
        return pattern_condition()

    parts: list[str] = []
    params: list = []
    for known in LABEL_PRESETS:
        for pattern in known["patterns"]:
            parts.append(
                f"(COALESCE(source_name, '') {op} {placeholder} OR COALESCE(channel_name, '') {op} {placeholder})"
            )
            params.extend([pattern, pattern])
    return f"NOT ({' OR '.join(parts)})", params


def search_label_songs(label: str, query: str = "", limit: int = 30) -> list[dict]:
    if not db_ready():
        return []

    label = (label or "").strip()
    q = (query or "").strip()
    if not label:
        return []

    placeholder = sql_placeholder()
    op = like_operator()
    label_condition, label_params = label_filter_sql(label)
    where_parts = [
        "embed_ok = 1",
        "video_id NOT IN (SELECT video_id FROM failed_videos)",
        label_condition,
    ]
    params: list = label_params

    if q:
        where_parts.append(f"(title {op} {placeholder} OR channel_name {op} {placeholder})")
        params.extend([f"%{q}%", f"%{q}%"])

    sql = f"""
        SELECT video_id, title, channel_name, thumbnail, score
        FROM karaoke_songs
        WHERE {" AND ".join(where_parts)}
        ORDER BY COALESCE(score, 0) DESC, title ASC
        LIMIT {placeholder}
    """

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, params + [limit])
        rows = cur.fetchall()

    return [
        {
            "videoId": row[0],
            "title": row[1],
            "channelName": row[2] or "",
            "thumbnail": row[3] or get_thumbnail(row[0]),
            "score": row[4] or 0,
        }
        for row in rows
    ]


def alphabet_sort_key(name: str) -> tuple[int, str]:
    first = (name or "").strip()[:1]
    if "\u0e00" <= first <= "\u0e7f":
        group = 0
    elif first.isascii() and first.isalpha():
        group = 1
    else:
        group = 2
    return (group, name.casefold())


def search_label_singers(label: str, limit: int = 200) -> list[dict]:
    if not db_ready():
        return []

    label = (label or "").strip()
    if not label:
        return []

    placeholder = sql_placeholder()
    label_condition, label_params = label_filter_sql(label)
    sql = f"""
        SELECT title, channel_name
        FROM karaoke_songs
        WHERE embed_ok = 1
          AND video_id NOT IN (SELECT video_id FROM failed_videos)
          AND {label_condition}
        LIMIT {placeholder}
    """

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, label_params + [2000])
        rows = cur.fetchall()

    counts: dict[str, int] = {}
    display_names: dict[str, str] = {}
    for title, channel_name in rows:
        for candidate in extract_singer_candidates(title or "", channel_name or "", ""):
            key = canonical_singer_key(candidate)
            counts[key] = counts.get(key, 0) + 1
            display_names.setdefault(key, candidate)

    ranked = sorted(
        counts.items(),
        key=lambda item: alphabet_sort_key(display_names[item[0]]),
    )
    return [
        {
            "name": display_names[key],
            "songCount": count,
        }
        for key, count in ranked[:limit]
    ]


def search_label_singer_songs(label: str, singer: str, limit: int = 30) -> list[dict]:
    if not db_ready():
        return []

    label = (label or "").strip()
    singer = (singer or "").strip()
    if not label or not singer:
        return []

    placeholder = sql_placeholder()
    op = like_operator()
    label_condition, label_params = label_filter_sql(label)
    sql = f"""
        SELECT video_id, title, channel_name, thumbnail, score
        FROM karaoke_songs
        WHERE embed_ok = 1
          AND video_id NOT IN (SELECT video_id FROM failed_videos)
          AND {label_condition}
          AND (title {op} {placeholder} OR channel_name {op} {placeholder})
        ORDER BY COALESCE(score, 0) DESC, title ASC
        LIMIT {placeholder}
    """

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, label_params + [f"%{singer}%", f"%{singer}%", limit])
        rows = cur.fetchall()

    return [
        {
            "videoId": row[0],
            "title": row[1],
            "channelName": row[2] or "",
            "thumbnail": row[3] or get_thumbnail(row[0]),
            "score": row[4] or 0,
        }
        for row in rows
    ]


def get_style_preset(style_id: str) -> dict | None:
    for preset in STYLE_PRESETS:
        if preset["id"] == style_id:
            return preset
    return None


def is_clean_style_song(title: str, channel_name: str = "", source_name: str = "") -> bool:
    title_text = (title or "").strip().lower()
    context_text = f"{title_text} {(channel_name or '').lower()} {(source_name or '').lower()}"

    if not any(signal in context_text for signal in STYLE_KARAOKE_SIGNALS):
        return False

    if any(text in title_text for text in STYLE_TITLE_TEXT_BLOCKLIST):
        return False

    return not any(
        re.search(pattern, title_text, flags=re.IGNORECASE)
        for pattern in STYLE_TITLE_BLOCKLIST
    )


def search_style_songs(style_id: str, limit: int = 30) -> list[dict]:
    if not db_ready():
        return []

    preset = get_style_preset(style_id)
    if not preset:
        return []

    placeholder = sql_placeholder()
    op = like_operator()
    keyword_parts: list[str] = []
    params: list = []

    for keyword in preset["keywords"]:
        keyword_parts.append(f"(title {op} {placeholder} OR channel_name {op} {placeholder} OR source_name {op} {placeholder})")
        like_value = f"%{keyword}%"
        params.extend([like_value, like_value, like_value])

    sql = f"""
        SELECT video_id, title, channel_name, thumbnail, score, source_name
        FROM karaoke_songs
        WHERE embed_ok = 1
          AND video_id NOT IN (SELECT video_id FROM failed_videos)
          AND ({" OR ".join(keyword_parts)})
        ORDER BY COALESCE(score, 0) DESC, title ASC
        LIMIT {placeholder}
    """
    fetch_limit = max(limit * 6, 80)

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, params + [fetch_limit])
        rows = cur.fetchall()

    results = []
    seen_video_ids = set()
    for row in rows:
        if row[0] in seen_video_ids:
            continue
        if not is_clean_style_song(row[1], row[2] or "", row[5] or ""):
            continue
        seen_video_ids.add(row[0])
        results.append(
            {
                "videoId": row[0],
                "title": row[1],
                "channelName": row[2] or "",
                "thumbnail": row[3] or get_thumbnail(row[0]),
                "score": row[4] or 0,
                "sourceName": row[5] or "",
            }
        )
        if len(results) >= limit:
            break

    return results


def search_style_label_songs(style_id: str, label_id: str, limit: int = 30) -> list[dict]:
    if not db_ready():
        return []

    preset = get_style_preset(style_id)
    label_preset = get_label_preset(label_id)
    if not preset or not label_preset:
        return []

    placeholder = sql_placeholder()
    op = like_operator()
    keyword_parts: list[str] = []
    params: list = []

    for keyword in preset["keywords"]:
        keyword_parts.append(f"(title {op} {placeholder} OR channel_name {op} {placeholder} OR source_name {op} {placeholder})")
        like_value = f"%{keyword}%"
        params.extend([like_value, like_value, like_value])

    label_condition, label_params = label_filter_sql(label_id)
    sql = f"""
        SELECT video_id, title, channel_name, thumbnail, score, source_name
        FROM karaoke_songs
        WHERE embed_ok = 1
          AND video_id NOT IN (SELECT video_id FROM failed_videos)
          AND {label_condition}
          AND ({" OR ".join(keyword_parts)})
        ORDER BY COALESCE(score, 0) DESC, title ASC
        LIMIT {placeholder}
    """
    fetch_limit = max(limit * 8, 120)

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, label_params + params + [fetch_limit])
        rows = cur.fetchall()

    results = []
    seen_video_ids = set()
    for row in rows:
        if row[0] in seen_video_ids:
            continue
        if not is_clean_style_song(row[1], row[2] or "", row[5] or ""):
            continue
        seen_video_ids.add(row[0])
        results.append(
            {
                "videoId": row[0],
                "title": row[1],
                "channelName": row[2] or "",
                "thumbnail": row[3] or get_thumbnail(row[0]),
                "score": row[4] or 0,
                "sourceName": row[5] or "",
            }
        )
        if len(results) >= limit:
            break

    return results


def infer_style_id_from_song(song: dict | None) -> str | None:
    if not song:
        return None

    context = " ".join(
        [
            str(song.get("title") or ""),
            str(song.get("channelName") or ""),
            str(song.get("sourceName") or ""),
        ]
    ).lower()

    if not context.strip():
        return None

    for preset in STYLE_PRESETS:
        for keyword in preset["keywords"]:
            if keyword and keyword.lower() in context:
                return preset["id"]

    return None


def infer_label_id_from_song(song: dict | None) -> str | None:
    if not song:
        return None

    context = " ".join(
        [
            str(song.get("channelName") or ""),
            str(song.get("sourceName") or ""),
        ]
    ).casefold()

    if not context.strip():
        return None

    for preset in LABEL_PRESETS:
        for pattern in preset["patterns"]:
            token = pattern.replace("%", "").casefold().strip()
            if token and token in context:
                return preset["id"]

    return "other"


def room_video_ids(room: dict) -> set[str]:
    ids = set()
    ids.update(room.get("used_video_ids", []))
    if room.get("current") and room["current"].get("videoId"):
        ids.add(room["current"]["videoId"])
    ids.update(
        item.get("videoId")
        for item in room.get("queue", [])
        if item.get("videoId")
    )
    ids.update(room.get("auto_suggest_history", []))
    return ids


def remember_room_video(room: dict, video_id: str) -> None:
    if not video_id:
        return

    used = room.setdefault("used_video_ids", [])
    if video_id not in used:
        used.append(video_id)
    del used[:-500]


def remember_user_selected_style(room: dict, song: dict) -> str | None:
    style_id = infer_style_id_from_song(song)
    if not style_id:
        return None

    styles = room.setdefault("user_selected_style_ids", [])
    if len(styles) < 3:
        styles.append(style_id)

    if len(styles) >= 3:
        counts = Counter(styles[:3])
        room["auto_suggest_style_id"] = max(
            counts,
            key=lambda item: (counts[item], -styles[:3].index(item)),
        )
    elif not room.get("auto_suggest_style_id"):
        room["auto_suggest_style_id"] = style_id

    return style_id


def remember_user_selected_label(room: dict, song: dict) -> str | None:
    label_id = infer_label_id_from_song(song)
    if not label_id:
        return None

    labels = room.setdefault("user_selected_label_ids", [])
    if len(labels) < 3:
        labels.append(label_id)

    if len(labels) >= 3:
        counts = Counter(labels[:3])
        room["auto_suggest_label_id"] = max(
            counts,
            key=lambda item: (counts[item], -labels[:3].index(item)),
        )
    elif not room.get("auto_suggest_label_id"):
        room["auto_suggest_label_id"] = label_id

    return label_id


def singer_keys_for_song(song: dict | None) -> set[str]:
    if not song:
        return set()
    title = str(song.get("title") or "")
    candidates = extract_singer_candidates(
        title,
        str(song.get("channelName") or ""),
        "",
    )
    title_before_pipe = re.split(r"\s*[|]\s*", title, maxsplit=1)[0]
    if " - " in title_before_pipe:
        candidates.append(title_before_pipe.rsplit(" - ", 1)[1])

    keys: set[str] = set()
    for candidate in candidates:
        pieces = re.split(r"\s*(?:&|,|/|และ|กับ| x | X |\+)\s*", candidate)
        for piece in pieces:
            cleaned = clean_singer_candidate(piece, "") or piece.strip()
            key = canonical_singer_key(cleaned)
            if key:
                keys.add(key)
    return keys


def remember_user_selected_singers(room: dict, song: dict) -> None:
    keys = singer_keys_for_song(song)
    if not keys:
        return
    singer_history = room.setdefault("user_selected_singer_keys", [])
    for key in keys:
        if key not in singer_history and len(singer_history) < 8:
            singer_history.append(key)


def compact_channel_key(value: str) -> str:
    text = re.sub(r"[^a-z0-9ก-๙]+", " ", (value or "").casefold())
    text = re.sub(r"\b(karaoke|official|music|channel|records|record|mv)\b", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def song_label_match(song: dict, label_id: str | None) -> bool:
    if not label_id:
        return False
    inferred = infer_label_id_from_song(song)
    return bool(inferred and inferred == label_id)


def song_style_match(song: dict, style_id: str | None) -> bool:
    if not style_id or style_id == "popular":
        return False
    inferred = infer_style_id_from_song(song)
    return bool(inferred and inferred == style_id)


def get_song_request_counts(video_ids: list[str]) -> dict[str, int]:
    if not db_ready() or not video_ids:
        return {}

    placeholder = sql_placeholder()
    placeholders = ", ".join([placeholder for _ in video_ids])
    with db_connect() as conn:
        ensure_song_request_stats_table(conn)
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT video_id, request_count
            FROM song_request_stats
            WHERE video_id IN ({placeholders})
            """,
            video_ids,
        )
        rows = cur.fetchall()

    return {row[0]: int(row[1] or 0) for row in rows}


def recent_auto_singer_keys(room: dict, lookback: int = 2) -> set[str]:
    keys: set[str] = set()
    recent_items = [
        item for item in room.get("queue", [])[-lookback:]
        if item.get("autoSuggested")
    ]
    if room.get("current") and room["current"].get("autoSuggested"):
        recent_items.append(room["current"])
    for item in recent_items:
        keys.update(singer_keys_for_song(item))
    return keys


def auto_singer_usage_counts(room: dict) -> Counter:
    counts: Counter = Counter()
    for key in room.get("auto_suggest_singer_keys", []):
        counts[key] += 1
    return counts


def auto_singer_key_set(room: dict) -> set[str]:
    return set(room.get("auto_suggest_singer_keys", []))


def remember_auto_suggest_singers(room: dict, song: dict) -> None:
    keys = singer_keys_for_song(song)
    if not keys:
        return
    history = room.setdefault("auto_suggest_singer_keys", [])
    history.extend(sorted(keys))
    del history[:-120]


def rank_auto_suggest_candidates(
    room: dict,
    candidates: list[dict],
    style_id: str | None,
    label_id: str | None,
) -> list[dict]:
    request_counts = get_song_request_counts([song["videoId"] for song in candidates])
    seed_singer_keys = set(room.get("user_selected_singer_keys", []))
    recent_singers = recent_auto_singer_keys(room)
    auto_usage = auto_singer_usage_counts(room)
    current_channel = compact_channel_key((room.get("current") or {}).get("channelName", ""))

    ranked = []
    preferred_countries, preferred_languages = locale_preferences(
        room.get("preferred_country") or "",
        room.get("preferred_language") or "",
        room.get("preferred_locale") or "",
    )
    locale_map = get_song_locale_map([song["videoId"] for song in candidates])

    for song in candidates:
        song.update(locale_map.get(song["videoId"], {}))
        score = float(song.get("score") or 0)
        request_count = request_counts.get(song["videoId"], 0)
        song_singers = singer_keys_for_song(song)
        channel_key = compact_channel_key(song.get("channelName") or "")

        if song_style_match(song, style_id):
            score += 900
        if seed_singer_keys and song_singers.intersection(seed_singer_keys):
            score += 260
        score += song_locale_score(song, preferred_countries, preferred_languages)
        if request_count:
            score += min(request_count, 25) * 12
        if channel_key and current_channel and channel_key == current_channel:
            score += 70

        if recent_singers and song_singers.intersection(recent_singers):
            score -= 900
        if song_singers:
            repeat_count = sum(auto_usage.get(key, 0) for key in song_singers)
            if repeat_count:
                score -= min(repeat_count, 6) * 320
        else:
            score -= 90
        if any(text in (song.get("title") or "").lower() for text in ("รวมเพลง", "รวมฮิต", "playlist")):
            score -= 600

        ranked.append((score, request_count, song))

    ranked.sort(key=lambda item: (-item[0], -item[1], item[2].get("title") or ""))
    return [song for _, _, song in ranked]


def choose_auto_suggest_song(room: dict) -> tuple[dict | None, str | None]:
    excluded = room_video_ids(room)
    seed_styles = room.get("user_selected_style_ids", [])[:3]
    if len(seed_styles) >= 3:
        counts = Counter(seed_styles)
        style_id = max(
            counts,
            key=lambda item: (counts[item], -seed_styles.index(item)),
        )
        room["auto_suggest_style_id"] = style_id
    else:
        style_id = room.get("auto_suggest_style_id")

    label_id = None

    if not style_id:
        style_id = infer_style_id_from_song(room.get("current"))

    candidates: list[dict] = []
    if style_id:
        candidates = [
            song for song in search_style_songs(style_id, limit=80)
            if song.get("videoId") not in excluded
        ]

    if not candidates:
        candidates = [
            song for song in get_recommended_songs(
                limit=10,
                country=room.get("preferred_country") or "",
                language=room.get("preferred_language") or "",
                locale=room.get("preferred_locale") or "",
            )
            if song.get("videoId") not in excluded
        ]
        style_id = style_id or "popular"

    if not candidates:
        return None, style_id

    ranked = rank_auto_suggest_candidates(room, candidates, style_id, label_id)
    used_auto_singers = auto_singer_key_set(room)
    diverse_ranked = [
        song for song in ranked
        if singer_keys_for_song(song) and not singer_keys_for_song(song).intersection(used_auto_singers)
    ]
    if diverse_ranked:
        return diverse_ranked[0], style_id

    return ranked[0], style_id


def append_room_queue_item(room: dict, song: dict, requested_by: str, auto_suggested: bool = False) -> dict:
    item_id = room["next_item_id"]
    room["next_item_id"] += 1

    item = {
        "itemId": item_id,
        "videoId": song["videoId"],
        "title": song["title"],
        "requestedBy": requested_by,
        "thumbnail": song["thumbnail"],
        "channelName": song.get("channelName") or "",
        "autoSuggested": auto_suggested,
    }

    if room["current"] is None:
        room["current"] = item
        room["is_playing"] = True
        room["player_nonce"] += 1
    else:
        room["queue"].append(item)

    remember_room_video(room, song["videoId"])
    return item


@app.route("/")
def index():
    return render_template(
        "index.html",
        db_exists=db_ready(),
        room_error=None,
        **room_capacity_context(),
    )


@app.route("/create", methods=["POST"])
def create_room():
    cleanup_inactive_rooms()
    if len(ROOMS) >= ROOM_MAX_ACTIVE:
        return (
            render_template(
                "index.html",
                db_exists=db_ready(),
                room_error="room_full",
                **room_capacity_context(),
            ),
            503,
        )

    code = generate_code()
    while code in ROOMS:
        code = generate_code()

    created_at = now_ms()
    ROOMS[code] = {
        "queue": [],
        "current": None,
        "is_playing": False,
        "player_nonce": 0,
        "next_item_id": 1,
        "reactions": [],
        "next_reaction_id": 1,
        "reaction_cooldowns": {},
        "auto_suggest_enabled": False,
        "auto_suggest_style_id": None,
        "auto_suggest_history": [],
        "user_selected_style_ids": [],
        "user_selected_label_ids": [],
        "user_selected_singer_keys": [],
        "auto_suggest_singer_keys": [],
        "user_selected_count": 0,
        "auto_suggest_label_id": None,
        "preferred_country": "",
        "preferred_language": "",
        "preferred_locale": "",
        "used_video_ids": [],
        "created_at": created_at,
        "last_seen_at": created_at,
    }
    return redirect(url_for("room_page", code=code))


@app.route("/join", methods=["POST"])
def join_room():
    code = request.form.get("room_code", "").strip().upper()
    if code in ROOMS:
        return redirect(url_for("mobile_page", code=code))
    return "Room not found", 404


@app.route("/room/<code>")
def room_page(code: str):
    room = get_room(code)
    if not room:
        return "Room not found", 404

    base_url = request.host_url.rstrip("/")

    return render_template(
        "room.html",
        code=code,
        base_url=base_url,
        db_exists=db_ready(),
    )


@app.route("/mobile/<code>")
def mobile_page(code: str):
    room = get_room(code)
    if not room:
        return "Room not found", 404
    return render_template("mobile.html", code=code, db_exists=db_ready())


@app.route("/api/rooms/<code>/state")
def api_room_state(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    cutoff = now_ms() - 15000
    room["reactions"] = [
        reaction for reaction in room.get("reactions", [])
        if int(reaction.get("createdAt", 0)) >= cutoff
    ]

    return jsonify(
        {
            "code": code,
            "current": room["current"],
            "queue": room["queue"],
            "is_playing": room["is_playing"],
            "player_nonce": room["player_nonce"],
            "queue_count": len(room["queue"]),
            "reactions": room["reactions"],
        }
    )


@app.route("/api/song/search")
def api_song_search():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    q = (request.args.get("q") or "").strip()
    limit_raw = (request.args.get("limit") or "20").strip()

    try:
        limit = max(1, min(int(limit_raw), 50))
    except ValueError:
        limit = 20

    if not q:
        return jsonify({"results": []})

    try:
        results = search_songs(q, limit=limit)
        return jsonify({"results": results})
    except Exception as e:
        print(f"SEARCH ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/song/refresh_missing", methods=["POST"])
def api_song_refresh_missing():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    data = request.get_json(silent=True) or {}
    q = normalize_refresh_query(data.get("q") or "")
    if len(q) < 2:
        return jsonify({"error": "search query required"}), 400

    job_id = create_search_refresh_job(q, request.remote_addr)
    try:
        found_count = discover_and_save_songs_for_query(q)
        results = search_songs(q, limit=20)
        finish_search_refresh_job(job_id, "done", found_count=found_count)
        return jsonify(
            {
                "success": True,
                "query": q,
                "foundCount": found_count,
                "results": results,
            }
        )
    except Exception as e:
        finish_search_refresh_job(job_id, "failed", error=str(e)[:500])
        print(f"REFRESH MISSING SONG ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/song/recommended")
def api_song_recommended():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    limit_raw = (request.args.get("limit") or "5").strip()
    try:
        limit = max(1, min(int(limit_raw), 10))
    except ValueError:
        limit = 5

    country = (request.args.get("country") or "")[:40]
    language = (request.args.get("language") or "")[:40]
    locale = (request.args.get("locale") or "")[:40]

    try:
        results = get_recommended_songs(limit=limit, country=country, language=language, locale=locale)
        return jsonify({"results": results})
    except Exception as e:
        print(f"RECOMMENDED SONGS ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/singer/search")
def api_singer_search():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    q = (request.args.get("q") or "").strip()
    limit_raw = (request.args.get("limit") or "20").strip()

    try:
        limit = max(1, min(int(limit_raw), 50))
    except ValueError:
        limit = 20

    if not q:
        return jsonify({"results": []})

    try:
        results = search_singers(q, limit=limit)
        return jsonify({"results": results})
    except Exception as e:
        print(f"SINGER SEARCH ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/singer/songs")
def api_singer_songs():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    singer = (request.args.get("singer") or "").strip()
    limit_raw = (request.args.get("limit") or "30").strip()

    try:
        limit = max(1, min(int(limit_raw), 50))
    except ValueError:
        limit = 30

    if not singer:
        return jsonify({"results": []})

    try:
        results = search_songs(singer, limit=limit)
        return jsonify({"results": results})
    except Exception as e:
        print(f"SINGER SONGS ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/label/search")
def api_label_search():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    q = (request.args.get("q") or "").strip()
    limit_raw = (request.args.get("limit") or "20").strip()

    try:
        limit = max(1, min(int(limit_raw), 50))
    except ValueError:
        limit = 20

    try:
        results = search_labels(q, limit=limit)
        return jsonify({"results": results})
    except Exception as e:
        print(f"LABEL SEARCH ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/label/songs")
def api_label_songs():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    label = (request.args.get("label") or "").strip()
    q = (request.args.get("q") or "").strip()
    limit_raw = (request.args.get("limit") or "30").strip()

    try:
        limit = max(1, min(int(limit_raw), 50))
    except ValueError:
        limit = 30

    if not label:
        return jsonify({"results": []})

    try:
        results = search_label_songs(label, query=q, limit=limit)
        return jsonify({"results": results})
    except Exception as e:
        print(f"LABEL SONGS ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/label/singers")
def api_label_singers():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    label = (request.args.get("label") or "").strip()
    limit_raw = (request.args.get("limit") or "200").strip()

    try:
        limit = max(1, min(int(limit_raw), 300))
    except ValueError:
        limit = 200

    if not label:
        return jsonify({"results": []})

    try:
        results = search_label_singers(label, limit=limit)
        return jsonify({"results": results})
    except Exception as e:
        print(f"LABEL SINGERS ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/label/singer_songs")
def api_label_singer_songs():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    label = (request.args.get("label") or "").strip()
    singer = (request.args.get("singer") or "").strip()
    limit_raw = (request.args.get("limit") or "30").strip()

    try:
        limit = max(1, min(int(limit_raw), 50))
    except ValueError:
        limit = 30

    if not label or not singer:
        return jsonify({"results": []})

    try:
        results = search_label_singer_songs(label, singer, limit=limit)
        return jsonify({"results": results})
    except Exception as e:
        print(f"LABEL SINGER SONGS ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/style/list")
def api_style_list():
    return jsonify(
        {
            "results": [
                {
                    "id": preset["id"],
                    "name": preset["name"],
                }
                for preset in STYLE_PRESETS
            ]
        }
    )


@app.route("/api/style/songs")
def api_style_songs():
    if not db_ready():
        return jsonify({"error": "database not configured"}), 500

    style_id = (request.args.get("style") or "").strip()
    limit_raw = (request.args.get("limit") or "30").strip()

    try:
        limit = max(1, min(int(limit_raw), 50))
    except ValueError:
        limit = 30

    preset = get_style_preset(style_id)
    if not preset:
        return jsonify({"error": "style not found"}), 404

    try:
        results = search_style_songs(style_id, limit=limit)
        return jsonify({"style": preset, "results": results})
    except Exception as e:
        print(f"STYLE SONGS ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/rooms/<code>/add", methods=["POST"])
def api_add_song(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    data = request.get_json(silent=True) or request.form
    requested_by = (data.get("requested_by") or "").strip()
    raw_url = (data.get("url") or "").strip()
    raw_video_id = (data.get("video_id") or "").strip()

    if not requested_by:
        return jsonify({"error": "please enter singer name"}), 400

    selected_song = None

    if raw_video_id:
        selected_song = get_song_by_video_id(raw_video_id)
        if not selected_song:
            return jsonify({"error": "song not found in database"}), 404
    else:
        video_id = extract_video_id(raw_url)
        if not video_id:
            return jsonify({"error": "invalid YouTube URL or video ID"}), 400

        selected_song = get_song_by_video_id(video_id)
        if not selected_song:
            selected_song = {
                "videoId": video_id,
                "title": f"YouTube Video ({video_id})",
                "channelName": "",
                "thumbnail": get_thumbnail(video_id),
                "score": 0,
                "embedOk": 1,
                "sourceName": "",
            }

    item_id = room["next_item_id"]
    room["next_item_id"] += 1

    item = {
        "itemId": item_id,
        "videoId": selected_song["videoId"],
        "title": selected_song["title"],
        "requestedBy": requested_by,
        "thumbnail": selected_song["thumbnail"],
        "channelName": selected_song.get("channelName") or "",
        "autoSuggested": False,
    }

    record_song_request(selected_song["videoId"])
    remember_room_video(room, selected_song["videoId"])
    room["user_selected_count"] = room.get("user_selected_count", 0) + 1
    remember_user_selected_style(room, selected_song)
    remember_user_selected_singers(room, selected_song)

    if room["current"] is None:
        room["current"] = item
        room["is_playing"] = True
        room["player_nonce"] += 1
        return jsonify(
            {
                "success": True,
                "message": "First song added and set to play",
                "current": room["current"],
                "queue_count": len(room["queue"]),
            }
        )

    room["queue"].append(item)
    return jsonify(
        {
            "success": True,
            "message": "Song added to queue",
            "current": room["current"],
            "queue_count": len(room["queue"]),
        }
    )


@app.route("/api/rooms/<code>/auto_suggest", methods=["POST"])
def api_auto_suggest_song(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))

    room.setdefault("auto_suggest_history", [])
    room.setdefault("user_selected_style_ids", [])
    room.setdefault("user_selected_label_ids", [])
    room.setdefault("user_selected_singer_keys", [])
    room.setdefault("auto_suggest_singer_keys", [])
    room.setdefault("user_selected_count", len(room.get("user_selected_style_ids", [])))
    room.setdefault("used_video_ids", [])
    room["preferred_country"] = (data.get("country") or room.get("preferred_country") or "")[:40]
    room["preferred_language"] = (data.get("language") or room.get("preferred_language") or "")[:40]
    room["preferred_locale"] = (data.get("locale") or room.get("preferred_locale") or "")[:40]
    room["auto_suggest_enabled"] = enabled
    if not enabled:
        return jsonify({"success": True, "enabled": False, "added": False})

    if room.get("user_selected_count", 0) < 3:
        return jsonify(
            {
                "success": True,
                "enabled": True,
                "added": False,
                "queue_count": len(room["queue"]),
                "reason": "waiting for first 3 user songs",
            }
        )

    if not room.get("current") and not room.get("auto_suggest_style_id"):
        return jsonify(
            {
                "success": True,
                "enabled": True,
                "added": False,
                "reason": "no style seed",
            }
        )

    if len(room.get("queue", [])) > 1:
        return jsonify(
            {
                "success": True,
                "enabled": True,
                "added": False,
                "queue_count": len(room["queue"]),
                "reason": "queue has enough songs",
            }
        )

    now = now_ms()
    if now - room.get("auto_suggest_last_added_at", 0) < 2500:
        return jsonify(
            {
                "success": True,
                "enabled": True,
                "added": False,
                "queue_count": len(room["queue"]),
                "reason": "cooldown",
            }
        )

    song, style_id = choose_auto_suggest_song(room)
    if not song:
        return jsonify(
            {
                "success": True,
                "enabled": True,
                "added": False,
                "queue_count": len(room["queue"]),
                "reason": "no candidate",
            }
        )

    item = append_room_queue_item(room, song, "Auto Suggest", auto_suggested=True)
    remember_auto_suggest_singers(room, song)
    room["auto_suggest_last_added_at"] = now
    room["auto_suggest_style_id"] = style_id
    history = room.setdefault("auto_suggest_history", [])
    history.append(song["videoId"])
    del history[:-500]

    return jsonify(
        {
            "success": True,
            "enabled": True,
            "added": True,
            "item": item,
            "style": get_style_preset(style_id) if style_id and style_id != "popular" else None,
            "queue_count": len(room["queue"]),
        }
    )


@app.route("/api/rooms/<code>/reaction", methods=["POST"])
def api_add_reaction(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    data = request.get_json(silent=True) or request.form
    requested_by = (data.get("requested_by") or "").strip()
    message = normalize_reaction_text(data.get("message") or "")

    if not requested_by:
        return reject_reaction("please enter singer name", "name_required")

    if not message:
        return reject_reaction("reaction is empty", "reaction_empty")

    if len(message) > REACTION_MAX_CHARS:
        return reject_reaction("reaction is too long", "reaction_too_long")

    if reaction_word_count(message) > REACTION_MAX_WORDS:
        return reject_reaction("reaction must be less than 10 words", "reaction_too_long")

    if REACTION_URL_RE.search(message):
        return reject_reaction("links are not allowed in reactions", "reaction_link_blocked")

    if REACTION_BLOCKED_RE.search(message):
        return reject_reaction("please keep reactions friendly", "reaction_blocked")

    current_ms = now_ms()
    cooldown_key = requested_by.casefold()
    cooldowns = room.setdefault("reaction_cooldowns", {})
    last_sent = int(cooldowns.get(cooldown_key, 0))
    if current_ms - last_sent < REACTION_COOLDOWN_MS:
        return reject_reaction("please wait before sending another reaction", "reaction_wait", 429)

    if room_reaction_rate_limited(room, current_ms):
        return reject_reaction("too many reactions right now", "reaction_wait", 429)

    cooldowns[cooldown_key] = current_ms
    reaction = {
        "id": room.get("next_reaction_id", 1),
        "message": message,
        "requestedBy": requested_by,
        "createdAt": current_ms,
    }
    room["next_reaction_id"] = reaction["id"] + 1
    room.setdefault("reactions", []).append(reaction)
    room["reactions"] = room["reactions"][-12:]

    return jsonify({"success": True, "reaction": reaction})


@app.route("/api/rooms/<code>/reorder_queue", methods=["POST"])
def api_reorder_queue(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    data = request.get_json(silent=True) or {}
    ordered_ids = data.get("ordered_ids")

    if not isinstance(ordered_ids, list):
        return jsonify({"error": "ordered_ids must be a list"}), 400

    current_queue = room["queue"]
    current_ids = [item["itemId"] for item in current_queue]

    try:
        ordered_ids_int = [int(x) for x in ordered_ids]
    except Exception:
        return jsonify({"error": "ordered_ids must be integers"}), 400

    if sorted(current_ids) != sorted(ordered_ids_int):
        return jsonify({"error": "queue ids mismatch"}), 400

    item_map = {item["itemId"]: item for item in current_queue}
    room["queue"] = [item_map[item_id] for item_id in ordered_ids_int]

    return jsonify({"success": True, "queue_count": len(room["queue"])})


@app.route("/api/rooms/<code>/delete", methods=["POST"])
def api_delete_queue_item(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    data = request.get_json(silent=True) or {}
    item_id = data.get("item_id")

    if item_id is None:
        return jsonify({"error": "item_id required"}), 400

    try:
        item_id = int(item_id)
    except Exception:
        return jsonify({"error": "invalid item_id"}), 400

    room["queue"] = [
        item for item in room["queue"]
        if item["itemId"] != item_id
    ]

    return jsonify({"success": True, "queue_count": len(room["queue"])})


@app.route("/api/rooms/<code>/play", methods=["POST"])
def api_play(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    if room["current"] is None:
        return jsonify({"error": "no current video"}), 400

    room["is_playing"] = True
    room["player_nonce"] += 1
    return jsonify({"success": True})


@app.route("/api/rooms/<code>/pause", methods=["POST"])
def api_pause(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    if room["current"] is None:
        return jsonify({"error": "no current video"}), 400

    room["is_playing"] = False
    return jsonify({"success": True})


@app.route("/api/rooms/<code>/skip", methods=["POST"])
def api_skip(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    if room["queue"]:
        room["current"] = room["queue"].pop(0)
        room["is_playing"] = True
        room["player_nonce"] += 1
        return jsonify(
            {
                "success": True,
                "message": "Skipped to next song",
                "current": room["current"],
                "queue_count": len(room["queue"]),
            }
        )

    room["current"] = None
    room["is_playing"] = False
    room["player_nonce"] += 1
    return jsonify(
        {
            "success": True,
            "message": "Queue ended",
            "current": None,
            "queue_count": 0,
        }
    )


@app.route("/api/rooms/<code>/report_player_error", methods=["POST"])
def api_report_player_error(code: str):
    room = get_room(code)
    if not room:
        return jsonify({"error": "room not found"}), 404

    data = request.get_json(silent=True) or {}
    video_id = extract_video_id(data.get("video_id")) if data.get("video_id") else None

    if not video_id and room["current"]:
        video_id = room["current"].get("videoId")

    if not video_id:
        return jsonify({"error": "video_id required"}), 400

    reason = str(data.get("reason") or "player_error")[:120]
    mark_video_failed(video_id, reason=reason)
    advance_room_after_failed_video(room, video_id)

    return jsonify(
        {
            "success": True,
            "video_id": video_id,
            "message": "Video blacklisted and skipped",
            "current": room["current"],
            "queue_count": len(room["queue"]),
        }
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
