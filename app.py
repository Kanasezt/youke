from __future__ import annotations

import os
import random
import re
import socket
import string
from contextlib import contextmanager

import psycopg
from flask import Flask, jsonify, redirect, render_template, request, url_for

app = Flask(__name__)

ROOMS: dict[str, dict] = {}
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()


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


def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def get_room(code: str) -> dict | None:
    return ROOMS.get(code)


def db_ready() -> bool:
    return bool(DATABASE_URL)


@contextmanager
def db_connect():
    conn = psycopg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def get_song_by_video_id(video_id: str) -> dict | None:
    if not db_ready():
        return None

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    video_id,
                    title,
                    channel_name,
                    thumbnail,
                    score,
                    embed_ok
                FROM karaoke_songs
                WHERE video_id = %s
                  AND embed_ok = 1
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
    }


def search_songs(query: str, limit: int = 20) -> list[dict]:
    if not db_ready():
        return []

    q = (query or "").strip()
    if not q:
        return []

    q_lower = q.lower()
    like_any = f"%{q_lower}%"
    like_prefix = f"{q_lower}%"

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    video_id,
                    title,
                    channel_name,
                    thumbnail,
                    score,
                    embed_ok,
                    CASE
                        WHEN lower(title) = %s THEN 400
                        WHEN lower(title) LIKE %s THEN 250
                        WHEN lower(title) LIKE %s THEN 150
                        WHEN lower(channel_name) LIKE %s THEN 80
                        ELSE 0
                    END
                    + COALESCE(score, 0) AS final_rank
                FROM karaoke_songs
                WHERE embed_ok = 1
                  AND (
                        lower(title) LIKE %s
                     OR lower(channel_name) LIKE %s
                  )
                ORDER BY final_rank DESC, title ASC
                LIMIT %s
                """,
                (
                    q_lower,
                    like_prefix,
                    like_any,
                    like_any,
                    like_any,
                    like_any,
                    limit,
                ),
            )
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


@app.route("/")
def index():
    local_ip = get_local_ip()
    return render_template("index.html", local_ip=local_ip, db_exists=db_ready())


@app.route("/create", methods=["POST"])
def create_room():
    code = generate_code()
    while code in ROOMS:
        code = generate_code()

    ROOMS[code] = {
        "queue": [],
        "current": None,
        "is_playing": False,
        "player_nonce": 0,
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

    local_ip = get_local_ip()
    base_url = f"http://{local_ip}:5000"
    return render_template(
        "room.html",
        code=code,
        base_url=base_url,
        local_ip=local_ip,
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

    return jsonify(
        {
            "code": code,
            "current": room["current"],
            "queue": room["queue"],
            "is_playing": room["is_playing"],
            "player_nonce": room["player_nonce"],
            "queue_count": len(room["queue"]),
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

    results = search_songs(q, limit=limit)
    return jsonify({"results": results})


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
            }

    item = {
        "videoId": selected_song["videoId"],
        "title": selected_song["title"],
        "requestedBy": requested_by,
        "thumbnail": selected_song["thumbnail"],
        "channelName": selected_song.get("channelName") or "",
    }

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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)