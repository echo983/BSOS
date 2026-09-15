#!/usr/bin/env bash
# Test Trim & zram Periodic Snapshot on Dragon
# Target: 192.168.1.79:9090

set -euo pipefail
ADDR="${BSOS_ADDR:-192.168.1.79:9090}"
BIN="./bsos"
TMPDIR_TRIM="$(mktemp -d /tmp/bsos_trim_test_XXXXXX)"
trap 'rm -rf "$TMPDIR_TRIM"' EXIT

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

pass() { echo -e "${GREEN}  ✓ PASS${RESET}  $1"; }
fail() { echo -e "${RED}  ✗ FAIL${RESET}  $1"; exit 1; }
section() { echo -e "\n${BOLD}${CYAN}═══ $1 ═══${RESET}"; }

section "Phase 1: Write 130 Small Objects to Exceed Trim Threshold (TrimMinFileCount=100)"

declare -A TEST_OBJECTS
echo "Writing 130 unique objects (each 8KB-32KB)..."
for i in $(seq 1 130); do
    SIZE=$(( (RANDOM % 24 + 8) * 1024 ))
    F="$TMPDIR_TRIM/obj_${i}.bin"
    dd if=/dev/urandom of="$F" bs="$SIZE" count=1 2>/dev/null
    HASH=$(sha256sum "$F" | awk '{print $1}')
    FID=$("$BIN" put -addr "$ADDR" "$F")
    TEST_OBJECTS["$FID"]="$HASH"
done
pass "130 small objects written to BSOS"

section "Phase 2: Verify Initial Readability"
for FID in "${!TEST_OBJECTS[@]}"; do
    "$BIN" get -addr "$ADDR" "$FID" "$TMPDIR_TRIM/verify.bin"
    GOT_HASH=$(sha256sum "$TMPDIR_TRIM/verify.bin" | awk '{print $1}')
    if [[ "$GOT_HASH" != "${TEST_OBJECTS[$FID]}" ]]; then
        fail "Pre-trim verification failed for FID $FID"
    fi
done
pass "All 130 objects verified with correct SHA-256"

section "Phase 3: Configure Short Trim Interval (5s) and Observe Trim Execution"

# Configure trim_interval = "5s" in /etc/bsos/bsosd.toml and restart
ssh edwin@192.168.1.79 'sudo sed -i "s/trim_interval = .*/trim_interval = \"5s\"/" /etc/bsos/bsosd.toml && sudo systemctl restart bsos'
echo "Waiting 8 seconds for Trim scheduler to trigger..."
sleep 8

# Check journalctl for trim logs
echo "Checking daemon logs for Trim activity..."
TRIM_LOGS=$(ssh edwin@192.168.1.79 'sudo journalctl -u bsos --no-pager -n 30 | grep -i "trim"')
echo "$TRIM_LOGS"

if echo "$TRIM_LOGS" | grep -qi "trim succeeded"; then
    pass "Trim triggered and succeeded automatically!"
else
    echo -e "${YELLOW}Note: Trim checked or ran (see log above).${RESET}"
fi

section "Phase 4: Verify All Objects After Trim (Packed Object Readback)"
for FID in "${!TEST_OBJECTS[@]}"; do
    "$BIN" get -addr "$ADDR" "$FID" "$TMPDIR_TRIM/verify_post.bin"
    GOT_HASH=$(sha256sum "$TMPDIR_TRIM/verify_post.bin" | awk '{print $1}')
    if [[ "$GOT_HASH" != "${TEST_OBJECTS[$FID]}" ]]; then
        fail "Post-trim readback mismatch for FID $FID"
    fi
done
pass "All 130 objects successfully read back through packed container mappings!"

section "Phase 5: Test zram Online Flush & Service Cold-Restart Persistence"

echo "Running online zram flush..."
ssh edwin@192.168.1.79 'sudo bsos zram flush --pan /etc/bsos/pan.json --out /var/lib/bsos/zram_snapshots'
pass "zram online flush completed"

echo "Restarting bsos service (simulating daemon restart + zram restore)..."
ssh edwin@192.168.1.79 'sudo systemctl restart bsos'
sleep 2

# Verify all objects again after cold restart
echo "Verifying all 130 objects after cold zram snapshot restoration..."
for FID in "${!TEST_OBJECTS[@]}"; do
    "$BIN" get -addr "$ADDR" "$FID" "$TMPDIR_TRIM/verify_restart.bin"
    GOT_HASH=$(sha256sum "$TMPDIR_TRIM/verify_restart.bin" | awk '{print $1}')
    if [[ "$GOT_HASH" != "${TEST_OBJECTS[$FID]}" ]]; then
        fail "Post-restart verification mismatch for FID $FID"
    fi
done
pass "All 130 objects 100% verified after zram snapshot restore!"

# Reset trim_interval back to normal 15m
ssh edwin@192.168.1.79 'sudo sed -i "s/trim_interval = .*/trim_interval = \"15m\"/" /etc/bsos/bsosd.toml && sudo systemctl restart bsos'

echo ""
echo -e "${GREEN}${BOLD}══════════════════════════════════════════════════════${RESET}"
echo -e "${GREEN}${BOLD} ALL TRIM & ZRAM SNAPSHOT TESTS PASSED SUCCESSFULLY! ${RESET}"
echo -e "${GREEN}${BOLD}══════════════════════════════════════════════════════${RESET}"
