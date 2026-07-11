# Market Data Pipeline

Real-time trade tick ingestion from Binance (websocket) into PostgreSQL,
everything running in Docker on a private network where the database only
accepts connections from the worker's internal IP.

## Quick start

```bash
scripts/run.sh            # cleanup, preflight checks, build + up
scripts/run.sh --fresh    # same, but also wipes the data volume
```

The script cleans up any previous run, checks that Docker is up, that the
Binance API answers (`ping` + `curl /api/v3/ping`), that port 5432 and the
Docker subnet are free, generates a `.env` with a random password if there
isn't one, builds the images and waits for the healthcheck.

## Layout

```
Binance WS ──wss──▶ mdp-ingestor (172.28.0.20) ──5432──▶ mdp-db (172.28.0.10)
                    private bridge network 172.28.0.0/24
                    5432 NOT published on the host
```

- `docker-compose.yml` — bridge network with a fixed subnet, static IPs, healthcheck
- `ingestor/` — Python worker: websocket → bounded buffer → batched transactional inserts
- `db/pg_hba.conf` — access only from 172.28.0.20/32 as user `ingestor` (SCRAM), everything else rejected
- `db/postgresql.conf` — explicit `fsync`/`synchronous_commit`, WAL tuning
- `db/init/` — `ticks` schema + least-privilege app user (INSERT/SELECT only)
- `scripts/run.sh` — bootstrap

## Consistency

The worker buffers ticks in memory and flushes in batches (up to
`BATCH_SIZE=500` ticks or every 2s), each batch in a single explicit
transaction. If the process dies between commits (OOM, kill -9, power
loss), Postgres rolls back whatever was in flight — the table only ever
holds complete batches, never half of one. `fsync` and
`synchronous_commit` are on, so a confirmed commit is actually on disk.

Replays are safe too: `UNIQUE(symbol, trade_id)` plus
`ON CONFLICT DO NOTHING` means re-sending ticks after a rollback or a
websocket reconnect doesn't create duplicates.

The buffer itself is a `deque(maxlen=50000)`: if the database stays down
for a long time, the oldest ticks get dropped (and logged) instead of the
process growing until it OOMs. On SIGTERM (`docker stop`) the worker does
a final committed flush before exiting.

## Network isolation

The `db` service has no `ports:` section, so 5432 doesn't exist on the
host at all — it's only reachable inside the `market_net` bridge network.
On top of that, `pg_hba.conf` allows exactly one route in: user
`ingestor`, database `market`, from `172.28.0.20/32`, SCRAM-SHA-256. Any
other TCP connection is explicitly rejected. The worker connects as a
least-privilege app user, not as the superuser.

Note: `POSTGRES_DB` must stay `market` — the name also appears in the
`db/pg_hba.conf` rule. If you change one, change both.

## Checking it works

```bash
docker compose logs -f ingestor        # committed batches, live
docker compose exec db psql -U admin -d market -c 'SELECT * FROM ticks_summary;'

# isolation proof: connecting from outside fails
psql -h 127.0.0.1 -p 5432 -U admin market   # -> connection refused
```
