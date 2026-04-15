from __future__ import annotations

import os
import random
import re
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

    terms = [t.strip() for t in q.split() if t.strip()]
    if not terms:
        return []

    where_parts: list[str] = ["embed_ok = 1"]
    params: list = []

    for term in terms:
        where_parts.append("(title ILIKE %s OR channel_name ILIKE %s)")
        params.extend([f"%{term}%", f"%{term}%"])

    full_q = q
    full_prefix = f"{q}%"
    full_any = f"%{q}%"

    ranking_sql = """
        CASE
            WHEN title ILIKE %s THEN 500
            WHEN title ILIKE %s THEN 350
            WHEN title ILIKE %s THEN 220
            WHEN channel_name ILIKE %s THEN 120
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
        LIMIT %s
    """

    with db_connect() as conn:
        with conn.cursor() as cur:
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


@app.route("/")
def index():
    return render_template("index.html", db_exists=db_ready())


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
        "next_item_id": 1,
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

    try:
        results = search_songs(q, limit=limit)
        return jsonify({"results": results})
    except Exception as e:
        print(f"SEARCH ERROR: {e}")
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
