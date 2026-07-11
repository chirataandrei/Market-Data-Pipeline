"""Binance trade tick ingestor.

Reads the combined trade stream over websocket, buffers ticks in memory
and flushes them to Postgres in batches, one transaction per batch.
If the process dies between commits, Postgres rolls back the in-flight
transaction, so the table only ever contains complete batches. The
UNIQUE(symbol, trade_id) constraint makes replays after a reconnect
harmless.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("ingestor")

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

SYMBOLS = [s.strip().lower() for s in os.environ.get("SYMBOLS", "btcusdt").split(",") if s.strip()]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SEC = float(os.environ.get("FLUSH_INTERVAL_SEC", "2"))
# hard cap on the in-memory buffer; beyond this we drop oldest instead of OOMing
MAX_BUFFER = int(os.environ.get("MAX_BUFFER", "50000"))

WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s}@trade" for s in SYMBOLS
)

INSERT_SQL = """
    INSERT INTO ticks (symbol, trade_id, price, quantity, trade_time, is_buyer_maker)
    VALUES %s
    ON CONFLICT (symbol, trade_id) DO NOTHING
"""


class Ingestor:
    def __init__(self) -> None:
        self.buffer: deque = deque(maxlen=MAX_BUFFER)
        self.dropped = 0
        self.total_inserted = 0
        self.stopping = asyncio.Event()
        self.conn = None

    def connect_db(self):
        """(Re)open the Postgres connection, retrying with backoff."""
        delay = 1.0
        while not self.stopping.is_set():
            try:
                conn = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    dbname=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    connect_timeout=5,
                )
                conn.autocommit = False
                log.info("connected to postgres %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
                return conn
            except psycopg2.OperationalError as exc:
                log.warning("db unavailable (%s), retrying in %.1fs", exc, delay)
                time.sleep(delay)  # runs on the executor thread, event loop is fine
                delay = min(delay * 2, 30)
        return None

    def flush_batch(self) -> int:
        """Write the current buffer to the db in a single transaction.

        On any error: rollback, drop the connection and put the rows back
        so the batch either lands whole or not at all.
        """
        if not self.buffer:
            return 0

        rows = []
        while self.buffer:
            rows.append(self.buffer.popleft())

        if self.conn is None or self.conn.closed:
            self.conn = self.connect_db()
            if self.conn is None:
                self._requeue(rows)
                return 0

        try:
            with self.conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=1000)
            self.conn.commit()
            self.total_inserted += len(rows)
            return len(rows)
        except psycopg2.Error as exc:
            log.error("batch failed, rolling back: %s", exc)
            try:
                self.conn.rollback()
            except psycopg2.Error:
                pass
            try:
                self.conn.close()
            except psycopg2.Error:
                pass
            self.conn = None
            self._requeue(rows)
            return 0

    def _requeue(self, rows) -> None:
        space = self.buffer.maxlen - len(self.buffer)
        kept = rows[-space:] if space < len(rows) else rows
        self.dropped += len(rows) - len(kept)
        self.buffer.extendleft(reversed(kept))
        if len(rows) != len(kept):
            log.warning("buffer full, dropped %d ticks (%d total)",
                        len(rows) - len(kept), self.dropped)

    async def consume_ws(self) -> None:
        while not self.stopping.is_set():
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    log.info("websocket connected: %s", WS_URL)
                    async for raw in ws:
                        if self.stopping.is_set():
                            break
                        msg = json.loads(raw)
                        d = msg.get("data", {})
                        if d.get("e") != "trade":
                            continue
                        if len(self.buffer) == self.buffer.maxlen:
                            self.dropped += 1
                        self.buffer.append((
                            d["s"].lower(),
                            int(d["t"]),
                            d["p"],
                            d["q"],
                            datetime.fromtimestamp(d["T"] / 1000, tz=timezone.utc),
                            bool(d["m"]),
                        ))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.stopping.is_set():
                    break
                log.warning("websocket dropped (%s), reconnecting in 3s", exc)
                await asyncio.sleep(3)

    async def flusher(self) -> None:
        """Flush every FLUSH_INTERVAL_SEC, or sooner once BATCH_SIZE is hit."""
        loop = asyncio.get_running_loop()
        last_report = loop.time()
        while not self.stopping.is_set():
            deadline = loop.time() + FLUSH_INTERVAL_SEC
            while loop.time() < deadline and len(self.buffer) < BATCH_SIZE:
                if self.stopping.is_set():
                    break
                await asyncio.sleep(0.05)

            # execute_values blocks, keep it off the event loop
            n = await loop.run_in_executor(None, self.flush_batch)
            if n:
                log.info("committed %d ticks (total %d, buffer %d, dropped %d)",
                         n, self.total_inserted, len(self.buffer), self.dropped)
            elif loop.time() - last_report > 30:
                log.info("no new ticks (total %d)", self.total_inserted)
                last_report = loop.time()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stopping.set)

        self.conn = await loop.run_in_executor(None, self.connect_db)

        tasks = [
            asyncio.create_task(self.consume_ws(), name="ws"),
            asyncio.create_task(self.flusher(), name="flusher"),
        ]
        await self.stopping.wait()
        log.info("shutting down, final flush...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        n = await loop.run_in_executor(None, self.flush_batch)
        log.info("final flush: %d ticks, %d total inserted", n, self.total_inserted)
        if self.conn and not self.conn.closed:
            self.conn.close()


if __name__ == "__main__":
    log.info("starting ingestion for: %s", ", ".join(SYMBOLS))
    asyncio.run(Ingestor().run())
