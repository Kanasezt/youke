from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import psycopg

SQLITE_PATH = Path("youke_karaoke.db")
SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

TABLES = [
    "source_channels",
    "karaoke_candidates",
    "karaoke_songs",
    "failed_videos",
]


def get_sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def fetch_rows(conn: sqlite3.Connection, table: str, columns: list[str]):
    col_sql = ", ".join(columns)
    cursor = conn.execute(f"SELECT {col_sql} FROM {table}")
    yield from cursor.fetchall()


def truncate_target_tables(pg_conn: psycopg.Connection) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            "TRUNCATE failed_videos, karaoke_songs, karaoke_candidates, source_channels RESTART IDENTITY;"
        )
    pg_conn.commit()


def normalize_row(table: str, columns: list[str], row: tuple) -> tuple:
    values = list(row)

    if table == "source_channels" and "is_exhausted" in columns:
        idx = columns.index("is_exhausted")
        raw = values[idx]
        if raw is None:
            values[idx] = False
        else:
            values[idx] = bool(raw)

    return tuple(values)


def insert_rows(pg_conn: psycopg.Connection, table: str, columns: list[str], rows: list[tuple]) -> None:
    if not rows:
        return

    normalized_rows = [normalize_row(table, columns, row) for row in rows]

    placeholders = ", ".join(["%s"] * len(columns))
    col_sql = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    with pg_conn.cursor() as cur:
        cur.executemany(sql, normalized_rows)
    pg_conn.commit()


def main() -> None:
    if not SQLITE_PATH.exists():
        raise FileNotFoundError(f"SQLite DB not found: {SQLITE_PATH}")

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    try:
        pg_conn = psycopg.connect(SUPABASE_DB_URL)
        try:
            truncate_target_tables(pg_conn)

            for table in TABLES:
                columns = get_sqlite_columns(sqlite_conn, table)
                rows = list(fetch_rows(sqlite_conn, table, columns))
                insert_rows(pg_conn, table, columns, rows)
                print(f"Migrated {table}: {len(rows)} rows")

        finally:
            pg_conn.close()
    finally:
        sqlite_conn.close()


if __name__ == "__main__":
    main()