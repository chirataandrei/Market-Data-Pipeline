-- Runs automatically on first init (docker-entrypoint-initdb.d).

CREATE TABLE IF NOT EXISTS ticks (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    trade_id        BIGINT      NOT NULL,
    price           NUMERIC(18, 8) NOT NULL,
    quantity        NUMERIC(18, 8) NOT NULL,
    trade_time      TIMESTAMPTZ NOT NULL,
    is_buyer_maker  BOOLEAN     NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- the same trade delivered twice (e.g. after a websocket reconnect)
    -- must not produce duplicates
    CONSTRAINT uq_ticks_symbol_trade UNIQUE (symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time ON ticks (symbol, trade_time DESC);

CREATE OR REPLACE VIEW ticks_summary AS
SELECT symbol,
       count(*)          AS num_ticks,
       min(trade_time)   AS first_tick,
       max(trade_time)   AS last_tick,
       avg(price)::numeric(18,8) AS avg_price
FROM ticks
GROUP BY symbol;
