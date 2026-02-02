# db.py

import sqlite3
from pathlib import Path
from datetime import datetime, date, timedelta

DB_PATH = Path("bot.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            status TEXT NOT NULL,

            entry_price REAL NOT NULL,
            amount REAL NOT NULL,
            leverage INTEGER NOT NULL,
            entry_margin REAL NOT NULL,

            entry_time TEXT NOT NULL,

            exit_price REAL,
            exit_time TEXT,
            pnl REAL DEFAULT 0,
            pnl_percent REAL DEFAULT 0,

            realised_pnl REAL DEFAULT 0,
            channel_message_id INTEGER
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day TEXT NOT NULL,
            total_pnl REAL NOT NULL,
            total_trades INTEGER NOT NULL,
            win_trades INTEGER NOT NULL
        );
        """
    )

    conn.commit()
    conn.close()


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    return {k: row[k] for k in row.keys()}


def _today_str() -> str:
    return date.today().isoformat()


def _week_ago_str() -> str:
    return (date.today() - timedelta(days=7)).isoformat()


def add_trade(
    symbol: str,
    side: str,
    entry_price: float,
    amount: float,
    leverage: int,
    entry_margin: float,
    entry_time: str | None = None,
    channel_message_id: int | None = None,
) -> int:
    conn = get_connection()
    cur = conn.cursor()
    now = entry_time or datetime.utcnow().isoformat()

    cur.execute(
        """
        INSERT INTO trades (
            symbol, side, status,
            entry_price, amount, leverage, entry_margin,
            entry_time,
            channel_message_id
        )
        VALUES (?, ?, 'open', ?, ?, ?, ?, ?, ?)
        """,
        (
            symbol,
            side,
            entry_price,
            amount,
            leverage,
            entry_margin,
            now,
            channel_message_id,
        ),
    )

    trade_id = cur.lastrowid
    conn.commit()
    conn.close()
    return trade_id


def update_trade_with_exit(
    trade_id: int, current_price: float, pnl: float, pnl_percent: float
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE trades
        SET exit_price = ?,
            pnl = ?,
            pnl_percent = ?
        WHERE id = ? AND status = 'open'
        """,
        (
            current_price,
            pnl,
            pnl_percent,
            trade_id,
        ),
    )

    conn.commit()
    conn.close()


def close_trade(
    symbol: str,
    side: str,
    exit_price: float,
    pnl: float,
    pnl_percent: float,
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT * FROM trades
        WHERE symbol = ? AND side = ? AND status = 'open'
        ORDER BY id DESC
        LIMIT 1
        """,
        (symbol, side),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return

    trade_id = row["id"]
    now = datetime.utcnow().isoformat()

    cur.execute(
        """
        UPDATE trades
        SET status = 'closed',
            exit_price = ?,
            exit_time = ?,
            pnl = ?,
            pnl_percent = ?,
            realised_pnl = ?
        WHERE id = ?
        """,
        (
            exit_price,
            now,
            pnl,
            pnl_percent,
            pnl,
            trade_id,
        ),
    )

    _update_daily_stats(conn, pnl)

    conn.commit()
    conn.close()


def get_open_trades() -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT * FROM trades
        WHERE status = 'open'
        ORDER BY id ASC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return [_row_to_dict(r) for r in rows]


def get_trade_by_symbol_and_side(symbol: str, side: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT * FROM trades
        WHERE symbol = ? AND side = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (symbol, side),
    )
    row = cur.fetchone()
    conn.close()
    return _row_to_dict(row) if row else None


def get_top_trades(limit: int = 10) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT * FROM trades
        WHERE status = 'closed'
        ORDER BY pnl DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return [_row_to_dict(r) for r in rows]


def clean_obsolete_open_trades(active_keys: list[tuple[str, str]]) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, symbol, side FROM trades
        WHERE status = 'open'
        """
    )
    rows = cur.fetchall()

    active_set = set(active_keys)
    to_delete_ids = [
        r["id"] for r in rows if (r["symbol"], r["side"]) not in active_set
    ]

    if to_delete_ids:
        cur.execute(
            f"DELETE FROM trades WHERE id IN ({','.join('?' * len(to_delete_ids))})",
            to_delete_ids,
        )

    deleted = len(to_delete_ids)
    conn.commit()
    conn.close()
    return deleted


def _update_daily_stats(conn: sqlite3.Connection, pnl: float) -> None:
    today = _today_str()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT * FROM stats
        WHERE day = ?
        """,
        (today,),
    )
    row = cur.fetchone()

    win = 1 if pnl > 0 else 0

    if row:
        total_pnl = float(row["total_pnl"]) + pnl
        total_trades = int(row["total_trades"]) + 1
        win_trades = int(row["win_trades"]) + win
        cur.execute(
            """
            UPDATE stats
            SET total_pnl = ?, total_trades = ?, win_trades = ?
            WHERE id = ?
            """,
            (total_pnl, total_trades, win_trades, row["id"]),
        )
    else:
        cur.execute(
            """
            INSERT INTO stats(day, total_pnl, total_trades, win_trades)
            VALUES (?, ?, ?, ?)
            """,
            (today, pnl, 1, win),
        )


def _stats_to_dict(row) -> dict:
    if not row:
        return {"total_pnl": 0.0, "total_trades": 0, "win_rate": 0.0}
    total_pnl = float(row["total_pnl"])
    total_trades = int(row["total_trades"])
    win_trades = int(row["win_trades"])
    win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0.0
    return {
        "total_pnl": total_pnl,
        "total_trades": total_trades,
        "win_rate": win_rate,
    }


def get_today_stats() -> dict:
    conn = get_connection()
    cur = conn.cursor()
    today = _today_str()

    cur.execute(
        """
        SELECT * FROM stats
        WHERE day = ?
        """,
        (today,),
    )
    row = cur.fetchone()
    conn.close()
    return _stats_to_dict(row)


def get_week_stats() -> dict:
    conn = get_connection()
    cur = conn.cursor()
    start = _week_ago_str()
    end = _today_str()

    cur.execute(
        """
        SELECT
            SUM(total_pnl) AS total_pnl,
            SUM(total_trades) AS total_trades,
            SUM(win_trades) AS win_trades
        FROM stats
        WHERE day BETWEEN ? AND ?
        """,
        (start, end),
    )
    row = cur.fetchone()
    conn.close()

    if row is None or row["total_trades"] is None:
        return {"total_pnl": 0.0, "total_trades": 0, "win_rate": 0.0}

    total_pnl = float(row["total_pnl"] or 0.0)
    total_trades = int(row["total_trades"] or 0)
    win_trades = int(row["win_trades"] or 0)
    win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0.0

    return {
        "total_pnl": total_pnl,
        "total_trades": total_trades,
        "win_rate": win_rate,
    }


def hard_reset_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM trades;")
    cur.execute("DELETE FROM stats;")
    conn.commit()
    conn.close()


init_db()
