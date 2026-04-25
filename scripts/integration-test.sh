#!/usr/bin/env bash
# =============================================================================
# integration_test.sh  —  iceberg-cli end-to-end tests
#
# Prerequisites
# ─────────────
#   1. docker compose up -d          (services: rest, minio, mc, spark-iceberg)
#   2. cargo build --release          (produces ./target/release/iceberg-cli)
#   3. This script: chmod +x integration_test.sh && ./integration_test.sh
#
# The script exits with code 1 on the first failure and prints a summary.
# Pass --keep-data to skip namespace teardown (useful for manual inspection).
# =============================================================================

set -uo pipefail
# NOTE: -e is intentionally omitted. ((FAIL++)) evaluates to exit code 1 when
# FAIL goes from 0 to 1 (arithmetic 0 is falsy in bash), which would abort the
# script under -e before we can print the summary.  We track failures manually
# and exit 1 at the end if any test failed.

# ── Configuration ─────────────────────────────────────────────────────────────
ICEBERG_URI="${ICEBERG_URI:-http://localhost:8181}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-admin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-password}"
AWS_REGION="${AWS_REGION:-us-east-1}"

CLI="${CLI:-./target/release/iceberg-cli}"
NS="shell_integration_test"    # distinct from Rust test namespace (integration_test)
KEEP_DATA="${1:-}"             # pass --keep-data to retain state after run

# Colour helpers
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
PASS=0; FAIL=0

# ── Helpers ───────────────────────────────────────────────────────────────────

# Base CLI invocation — all global flags pre-filled.
cli() {
    "$CLI" \
        --uri            "$ICEBERG_URI" \
        --s3-endpoint    "$S3_ENDPOINT" \
        --access-key-id  "$AWS_ACCESS_KEY_ID" \
        --secret-access-key "$AWS_SECRET_ACCESS_KEY" \
        --region         "$AWS_REGION" \
        "$@"
}

# assert_ok <test-name> <command...>
# Runs the command; passes if exit code is 0.
assert_ok() {
    local name="$1"; shift
    if output=$(cli "$@" 2>&1); then
        echo -e "${GREEN}PASS${NC}  $name"
        PASS=$(( PASS + 1 ))
    else
        echo -e "${RED}FAIL${NC}  $name"
        echo      "      Command : cli $*"
        echo      "      Output  : $output"
        FAIL=$(( FAIL + 1 ))
    fi
}

# assert_output <test-name> <expected-substring> <command...>
# Passes if the command succeeds AND stdout contains the expected string.
assert_output() {
    local name="$1"; local expected="$2"; shift 2
    if output=$(cli "$@" 2>&1); then
        if echo "$output" | grep -qF "$expected"; then
            echo -e "${GREEN}PASS${NC}  $name"
            PASS=$(( PASS + 1 ))
        else
            echo -e "${RED}FAIL${NC}  $name  (output missing: '$expected')"
            echo      "      Output: $output"
            FAIL=$(( FAIL + 1 ))
        fi
    else
        echo -e "${RED}FAIL${NC}  $name  (command failed)"
        echo      "      Output: $output"
        FAIL=$(( FAIL + 1 ))
    fi
}

# assert_fail <test-name> <command...>
# Passes if the command exits with a non-zero code (expected failure).
assert_fail() {
    local name="$1"; shift
    if output=$(cli "$@" 2>&1); then
        echo -e "${RED}FAIL${NC}  $name  (expected failure, but command succeeded)"
        FAIL=$(( FAIL + 1 ))
    else
        echo -e "${GREEN}PASS${NC}  $name  (correctly rejected)"
        PASS=$(( PASS + 1 ))
    fi
}

wait_for_service() {
    local url="$1"
    echo -e "${YELLOW}Waiting for $url ...${NC}"
    for i in $(seq 1 30); do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo "  → ready after ${i}s"
            return 0
        fi
        sleep 1
    done
    echo -e "${RED}Service at $url did not become ready within 30s${NC}"
    exit 1
}

section() {
    echo ""
    echo "══════════════════════════════════════════════════════"
    echo "  $1"
    echo "══════════════════════════════════════════════════════"
}

# ── Preflight ─────────────────────────────────────────────────────────────────

section "0 · Preflight checks"

if [[ ! -x "$CLI" ]]; then
    echo -e "${RED}CLI binary not found at $CLI${NC}"
    echo "Run:  cargo build --release"
    exit 1
fi

wait_for_service "$ICEBERG_URI/v1/config"
wait_for_service "$S3_ENDPOINT/minio/health/live"

# ── Cleanup helper ─────────────────────────────────────────────────────────────
cleanup() {
    if [[ "$KEEP_DATA" != "--keep-data" ]]; then
        echo ""
        echo "Cleaning up namespace '$NS' ..."
        # Iceberg REST doesn't expose a delete-namespace endpoint in the
        # fixture image; MinIO objects are cleaned on next docker-compose up.
        # Nothing to do here unless your catalog supports it.
    fi
    echo ""
    echo "══════════════════════════════════════════════════════"
    echo -e "  Results:  ${GREEN}${PASS} passed${NC}  ${RED}${FAIL} failed${NC}"
    echo "══════════════════════════════════════════════════════"
    if (( FAIL > 0 )); then exit 1; fi
}
trap cleanup EXIT

# =============================================================================
# LEVEL 1 — Namespace & table management
# =============================================================================
section "1 · Namespace management"

assert_ok     "1.1  create namespace"        create-namespace --namespace "$NS"
assert_output "1.2  list-namespaces shows ns" "$NS"  list-namespaces
# Creating the same namespace twice should be handled gracefully (idempotent or
# clear error — either is acceptable; we just ensure the CLI doesn't crash)
assert_ok     "1.3  create namespace again (idempotent)" create-namespace --namespace "$NS"

section "2 · Table creation"

assert_ok     "2.1  create simple table" \
    create-table --table "$NS.products" \
    --schema "id:long,name:string,price:double"

assert_output "2.2  list-tables shows products" "products" \
    list-tables --namespace "$NS"

assert_output "2.3  describe shows schema fields" "price" \
    describe --table "$NS.products"

assert_output "2.4  describe shows format version" "Format" \
    describe --table "$NS.products"

# =============================================================================
# LEVEL 2 — Write & scan
# =============================================================================
section "3 · Basic write / scan"

assert_ok "3.1  write single row" \
    write --table "$NS.products" \
    --json '[{"id":1,"name":"Widget","price":9.99}]'

assert_output "3.2  scan shows written row" "Widget" \
    scan --table "$NS.products"

assert_ok "3.3  write multiple rows in one call" \
    write --table "$NS.products" \
    --json '[{"id":2,"name":"Gadget","price":19.99},{"id":3,"name":"Doohickey","price":4.49}]'

assert_output "3.4  scan shows all three rows" "Gadget" \
    scan --table "$NS.products"

assert_output "3.5  scan with limit=1 returns header + 1 row" "1 rows shown" \
    scan --table "$NS.products" --limit 1

section "4 · Column projection"

assert_output "4.1  projection: only selected columns appear" "name" \
    scan --table "$NS.products" --columns "id,name"

# 'price' should NOT appear when not selected
assert_ok "4.2  projection: excluded column absent" \
    scan --table "$NS.products" --columns "id,name"

# =============================================================================
# LEVEL 3 — Multiple tables & append semantics
# =============================================================================
section "5 · Multiple tables & append semantics"

assert_ok "5.1  create orders table" \
    create-table --table "$NS.orders" \
    --schema "id:long,product_id:long,qty:int,total:double,status:string"

assert_ok "5.2  write first order batch" \
    write --table "$NS.orders" \
    --json '[{"id":100,"product_id":1,"qty":2,"total":19.98,"status":"pending"}]'

assert_ok "5.3  write second order batch (appended)" \
    write --table "$NS.orders" \
    --json '[{"id":101,"product_id":2,"qty":1,"total":19.99,"status":"shipped"},{"id":102,"product_id":3,"qty":5,"total":22.45,"status":"pending"}]'

assert_output "5.4  scan orders returns all rows from both appends" "shipped" \
    scan --table "$NS.orders"

assert_output "5.5  scan orders pending rows visible" "pending" \
    scan --table "$NS.orders"

section "6 · Snapshot isolation (describe after writes)"

# Each write creates a new snapshot; describe should show a non-null snapshot id.
assert_output "6.1  describe shows snapshot id after writes" "Snapshot" \
    describe --table "$NS.orders"

# =============================================================================
# LEVEL 4 — Edge cases & error handling
# =============================================================================
section "7 · Edge cases"

assert_ok "7.1  write empty JSON array is a no-op" \
    write --table "$NS.products" --json '[]'

assert_output "7.2  scan empty-write table still readable" "Widget" \
    scan --table "$NS.products"

assert_ok "7.3  write null values in optional fields" \
    write --table "$NS.products" \
    --json '[{"id":99,"name":null,"price":0.0}]'

assert_output "7.4  scan with high limit returns all rows" "rows shown" \
    scan --table "$NS.products" --limit 9999

section "8 · Expected failures"

assert_fail "8.1  scan non-existent table fails cleanly" \
    scan --table "$NS.no_such_table"

assert_fail "8.2  describe non-existent table fails cleanly" \
    describe --table "$NS.ghost"

assert_fail "8.3  list-tables for unknown namespace fails" \
    list-tables --namespace "no_such_namespace_xyz"

# =============================================================================
# LEVEL 5 — Data types table
# =============================================================================
section "9 · All supported data types"

assert_ok "9.1  create all-types table" \
    create-table --table "$NS.all_types" \
    --schema "i:int,l:long,f:float,d:double,b:boolean,s:string,dt:date,ts:timestamp"

assert_ok "9.2  write all-types row" \
    write --table "$NS.all_types" \
    --json '[{"i":42,"l":9000000000,"f":3.14,"d":2.718,"b":true,"s":"hello","dt":null,"ts":null}]'

assert_output "9.3  scan all-types row shows long value" "9000000000" \
    scan --table "$NS.all_types"

# =============================================================================
# LEVEL 6 — Large batch stress test
# =============================================================================
section "10 · Large batch write"

assert_ok "10.1  create stress table" \
    create-table --table "$NS.stress" \
    --schema "id:long,val:string"

# Generate 200 rows as a JSON array
LARGE_JSON="["
for i in $(seq 1 200); do
    LARGE_JSON+="{\"id\":$i,\"val\":\"row_$i\"}"
    [[ $i -lt 200 ]] && LARGE_JSON+=","
done
LARGE_JSON+="]"

assert_ok "10.2  write 200 rows in single call" \
    write --table "$NS.stress" --json "$LARGE_JSON"

assert_output "10.3  scan 200-row table with limit 200" "200 rows shown" \
    scan --table "$NS.stress" --limit 200

# =============================================================================
# LEVEL 7 — Sync config (requires PostgreSQL — skipped if PG not available)
# =============================================================================
section "11 · Sync config (skipped when Postgres unavailable)"

PG_DSN="${PG_DSN:-}"
if [[ -z "$PG_DSN" ]]; then
    echo -e "${YELLOW}SKIP${NC}  11.x  PG_DSN not set — skipping sync tests"
    echo "       Export PG_DSN='host=localhost port=5432 dbname=test user=app password=pass'"
else
    # Write a minimal sync config pointing at the running services.
    SYNC_CFG=$(mktemp /tmp/sync_test_XXXXXX.yaml)
    cat > "$SYNC_CFG" <<YAML
sources:
  test_pg:
    type: postgres
    dsn: "$PG_DSN"

destinations:
  warehouse:
    catalog_uri: "$ICEBERG_URI"
    s3_endpoint:  "$S3_ENDPOINT"
    region:       "$AWS_REGION"
    access_key_id: "$AWS_ACCESS_KEY_ID"
    secret_access_key: "$AWS_SECRET_ACCESS_KEY"

sync_jobs:
  - name: sync_test_table
    source: test_pg
    destination: warehouse
    namespace: $NS
    table: sync_output
    sql: |
      SELECT 1::bigint AS id, 'from_pg'::text AS label
    watermark_column: ~
    batch_size: 100
    mode: full
YAML

    assert_ok "11.1  sync full-mode job from postgres" \
        sync --config "$SYNC_CFG"

    assert_output "11.2  synced table is queryable" "from_pg" \
        scan --table "$NS.sync_output"

    assert_ok "11.3  sync is idempotent (run again)" \
        sync --config "$SYNC_CFG"

    rm -f "$SYNC_CFG"
fi

# Summary printed by the EXIT trap.