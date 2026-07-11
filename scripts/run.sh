#!/usr/bin/env bash
# Bootstrap: clean up the old environment, check preconditions (docker,
# ports, Binance API reachable), then build and start the whole stack.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[0;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[ OK ]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
die()  { echo -e "${RED}[FAIL]${NC} $*" >&2; exit 1; }

FRESH=0
[[ "${1:-}" == "--fresh" ]] && FRESH=1   # --fresh also wipes the data volume

# --- docker daemon ---------------------------------------------------
command -v docker >/dev/null || die "docker is not installed"
if ! docker info >/dev/null 2>&1; then
    if [[ "$(uname)" == "Darwin" ]]; then
        warn "docker daemon not running, starting Docker Desktop..."
        open -a Docker || die "could not start Docker Desktop"
        for _ in $(seq 1 60); do
            docker info >/dev/null 2>&1 && break
            sleep 2
        done
    fi
    docker info >/dev/null 2>&1 || die "docker daemon unavailable"
fi
ok "docker daemon up ($(docker version --format '{{.Server.Version}}'))"

# --- cleanup ---------------------------------------------------------
echo "--- cleaning up old environment..."
if [[ $FRESH -eq 1 ]]; then
    docker compose down --volumes --remove-orphans 2>/dev/null || true
    ok "environment wiped, including the data volume"
else
    docker compose down --remove-orphans 2>/dev/null || true
    ok "old containers/network stopped (data volume kept, use --fresh to wipe)"
fi

# --- network / port checks -------------------------------------------
echo "--- checking network and ports..."

# icmp is often blocked by CDNs, so ping is informational only;
# the curl against the REST endpoint is the check that matters
if ping -c 1 -t 3 api.binance.com >/dev/null 2>&1; then
    ok "ping api.binance.com responds"
else
    warn "ping blocked/no reply (typical for CDNs), curl decides"
fi

HTTP_CODE=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 \
    https://api.binance.com/api/v3/ping || echo "000")
[[ "$HTTP_CODE" == "200" ]] || die "Binance API not responding (HTTP $HTTP_CODE), check your connection"
ok "Binance API alive (GET /api/v3/ping -> HTTP 200)"

# 5432 isn't published by this compose file, but a stray local postgres
# is worth knowing about
if lsof -nP -iTCP:5432 -sTCP:LISTEN >/dev/null 2>&1; then
    warn "port 5432 in use on the host by another process (doesn't affect us: our db doesn't publish the port)"
else
    ok "port 5432 free on the host"
fi

# make sure our private subnet isn't already taken by another docker network
CONFLICT=$(docker network ls -q | xargs -r docker network inspect --format \
    '{{.Name}} {{range .IPAM.Config}}{{.Subnet}}{{end}}' 2>/dev/null \
    | awk '$2=="172.28.0.0/24" && $1!="market-data-pipeline_market_net" {print $1}')
[[ -z "$CONFLICT" ]] || die "subnet 172.28.0.0/24 already used by docker network '$CONFLICT'"
ok "private subnet 172.28.0.0/24 available"

# --- secrets ----------------------------------------------------------
if [[ ! -f .env ]]; then
    echo "--- generating .env with a random password..."
    # not "tr </dev/urandom | head": head closing the pipe kills tr with
    # SIGPIPE, which under pipefail+errexit kills the whole script
    PASS=$(openssl rand -hex 16)
    sed "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=${PASS}/" .env.example > .env
    ok ".env created (random password, gitignored)"
else
    ok "reusing existing .env"
fi

# --- build + up --------------------------------------------------------
echo "--- building images..."
docker compose build --pull
ok "build done"

echo "--- starting the stack..."
docker compose up -d

echo "--- waiting for postgres to become healthy..."
for _ in $(seq 1 30); do
    STATUS=$(docker inspect --format '{{.State.Health.Status}}' mdp-db 2>/dev/null || echo "starting")
    [[ "$STATUS" == "healthy" ]] && break
    sleep 2
done
[[ "$STATUS" == "healthy" ]] || die "postgres never became healthy, see: docker compose logs db"
ok "postgres healthy"

# --- sanity check: db must not be reachable from outside ---------------
echo "--- checking db isolation..."
if nc -z -G 2 127.0.0.1 5432 2>/dev/null && \
   docker port mdp-db 2>/dev/null | grep -q 5432; then
    die "port 5432 is published on the host -- it should not be!"
fi
ok "5432 not exposed on the host; access only from 172.28.0.20 (pg_hba)"

echo
docker compose ps
echo
ok "done. useful commands:"
echo "    docker compose logs -f ingestor                 # watch ingestion live"
echo "    docker compose exec db psql -U admin -d market -c 'SELECT * FROM ticks_summary;'"
echo "    scripts/run.sh --fresh                          # full reset, data included"
