#!/usr/bin/env bash
# BSOS Comprehensive End-to-End Test Suite
# Target: 192.168.1.79:9090 (dragon)
# Run from /home/edwin/ramws/any/BSOS

ADDR="${BSOS_ADDR:-192.168.1.79:9090}"
BIN="./bsos"
TMPDIR_E2E="$(mktemp -d /tmp/bsos_e2e_XXXXXX)"
trap 'rm -rf "$TMPDIR_E2E"' EXIT

PASS=0
FAIL=0
FAIL_MSGS=()

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

pass()    { echo -e "${GREEN}  ✓ PASS${RESET}  $1"; ((PASS++)) || true; }
fail()    { echo -e "${RED}  ✗ FAIL${RESET}  $1"; ((FAIL++)) || true; FAIL_MSGS+=("$1"); }
section() { echo -e "\n${BOLD}${CYAN}═══ $1 ═══${RESET}"; }

bsos_put()  { "$BIN" put  -addr "$ADDR" "$@"; }
bsos_get()  { "$BIN" get  -addr "$ADDR" "$@"; }
bsos_head() { "$BIN" head -addr "$ADDR" "$@"; }

echo -e "${BOLD}BSOS E2E Test Suite — target: $ADDR${RESET}"
echo "Scratch dir: $TMPDIR_E2E"

# ══════════════════════════════════════════════════════════════════════════════
section "1. Daemon Liveness"

echo -n "  1.1 health check ... "
HEALTH=$(  "$BIN" health -addr "$ADDR" 2>&1 ) && \
[[ "$HEALTH" == "OK" ]] && pass "health → OK" || fail "health → '$HEALTH' (daemon down?)"

echo -n "  1.2 bonnie capacity estimate ... "
BONNIE=$( "$BIN" bonnie -addr "$ADDR" -json 2>&1 )
POW2=$(echo "$BONNIE" | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['ch_d_pow2'])" 2>/dev/null || echo "0")
MAX=$(echo "$BONNIE"  | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['max_bytes'])"  2>/dev/null || echo "0")
[[ "$POW2" -ge 20 ]] && pass "bonnie: ch_d_pow2=$POW2  max_bytes=$MAX" || fail "bonnie low capacity: $BONNIE"

# ══════════════════════════════════════════════════════════════════════════════
section "2. Tiny Objects (stdin/pipe — zram tier)"

echo -n "  2.1 empty payload rejection ... "
EMPTY_RES=$( echo -n "" | "$BIN" put -addr "$ADDR" - 2>&1 || true )
echo "$EMPTY_RES" | grep -q "E_INVALID_ARGUMENT" && pass "empty payload → E_INVALID_ARGUMENT" || fail "empty: got '$EMPTY_RES'"

echo -n "  2.2 1-byte object round-trip ... "
printf 'X' > "$TMPDIR_E2E/one_byte"
FID_1B=$( bsos_put "$TMPDIR_E2E/one_byte" 2>&1 )
GOT=$(     bsos_get "$FID_1B" - 2>&1 )
[[ "$GOT" == "X" ]] && pass "1-byte round-trip FID=$FID_1B" || fail "1-byte mismatch: '$GOT'"

echo -n "  2.3 stdin pipe string ... "
TEXT="Hello dragon @ $(date --utc +%T)"
FID_STR=$( echo -n "$TEXT" | bsos_put - )
RET_STR=$( bsos_get "$FID_STR" - )
[[ "$RET_STR" == "$TEXT" ]] && pass "stdin pipe FID=$FID_STR" || fail "stdin mismatch: '$RET_STR'"

echo -n "  2.4 head size matches ... "
SZ=$( bsos_head -json "$FID_STR" | python3 -c "import json,sys;print(json.load(sys.stdin)['size'])" 2>/dev/null || echo -1 )
[[ "$SZ" -eq "${#TEXT}" ]] && pass "head size=$SZ" || fail "head size=$SZ expected=${#TEXT}"

echo -n "  2.5 4 KB random sha256 ... "
dd if=/dev/urandom of="$TMPDIR_E2E/4k.bin" bs=4096 count=1 2>/dev/null
ORIG=$( sha256sum "$TMPDIR_E2E/4k.bin" | awk '{print $1}' )
FID_4K=$( bsos_put "$TMPDIR_E2E/4k.bin" )
bsos_get "$FID_4K" "$TMPDIR_E2E/4k_ret.bin"
RET=$( sha256sum "$TMPDIR_E2E/4k_ret.bin" | awk '{print $1}' )
[[ "$ORIG" == "$RET" ]] && pass "4KB sha256 match FID=$FID_4K" || fail "4KB sha256 mismatch"

echo -n "  2.6 512 KB random sha256 ... "
dd if=/dev/urandom of="$TMPDIR_E2E/512k.bin" bs=512K count=1 2>/dev/null
ORIG=$( sha256sum "$TMPDIR_E2E/512k.bin" | awk '{print $1}' )
FID_512K=$( bsos_put "$TMPDIR_E2E/512k.bin" )
bsos_get "$FID_512K" "$TMPDIR_E2E/512k_ret.bin"
RET=$( sha256sum "$TMPDIR_E2E/512k_ret.bin" | awk '{print $1}' )
[[ "$ORIG" == "$RET" ]] && pass "512KB sha256 match FID=$FID_512K" || fail "512KB sha256 mismatch"

# ══════════════════════════════════════════════════════════════════════════════
section "3. Large Objects (NVMe, O_DIRECT)"

for SZ_MB in 1 5 32 128; do
    echo -n "  3.x ${SZ_MB} MB sha256 round-trip ... "
    F="$TMPDIR_E2E/${SZ_MB}mb.bin"
    dd if=/dev/urandom of="$F" bs=1M count="$SZ_MB" 2>/dev/null
    ORIG=$( sha256sum "$F" | awk '{print $1}' )
    FID=$( bsos_put "$F" )
    bsos_get "$FID" "${F}.ret"
    RET=$( sha256sum "${F}.ret" | awk '{print $1}' )
    [[ "$ORIG" == "$RET" ]] && pass "${SZ_MB}MB sha256 match FID=$FID" || fail "${SZ_MB}MB sha256 mismatch"
done

# ══════════════════════════════════════════════════════════════════════════════
section "4. Range Reads"

# Known-layout: 1024 x 0x41 ('A') then 1024 x 0x42 ('B')
python3 -c "import sys; sys.stdout.buffer.write(b'A'*1024 + b'B'*1024)" > "$TMPDIR_E2E/range_src.bin"
FID_RANGE=$( bsos_put "$TMPDIR_E2E/range_src.bin" )

echo -n "  4.1 range [0,1024) → all A's ... "
bsos_get -range 0-1024 "$FID_RANGE" "$TMPDIR_E2E/range_a.bin"
EA=$( python3 -c "import sys; sys.stdout.buffer.write(b'A'*1024)" | sha256sum | awk '{print $1}' )
GA=$( sha256sum "$TMPDIR_E2E/range_a.bin" | awk '{print $1}' )
[[ "$EA" == "$GA" ]] && pass "range [0,1024)" || fail "range [0,1024) mismatch"

echo -n "  4.2 range [1024,2048) → all B's ... "
bsos_get -range 1024-2048 "$FID_RANGE" "$TMPDIR_E2E/range_b.bin"
EB=$( python3 -c "import sys; sys.stdout.buffer.write(b'B'*1024)" | sha256sum | awk '{print $1}' )
GB=$( sha256sum "$TMPDIR_E2E/range_b.bin" | awk '{print $1}' )
[[ "$EB" == "$GB" ]] && pass "range [1024,2048)" || fail "range [1024,2048) mismatch"

echo -n "  4.3 range [512,1536) spanning boundary ... "
bsos_get -range 512-1536 "$FID_RANGE" "$TMPDIR_E2E/range_ab.bin"
EAB=$( python3 -c "import sys; sys.stdout.buffer.write(b'A'*512+b'B'*512)" | sha256sum | awk '{print $1}' )
GAB=$( sha256sum "$TMPDIR_E2E/range_ab.bin" | awk '{print $1}' )
[[ "$EAB" == "$GAB" ]] && pass "range [512,1536) boundary" || fail "range [512,1536) mismatch"

echo -n "  4.4 range on 5MB NVMe object [1MB, 1MB+4096) ... "
dd if=/dev/urandom of="$TMPDIR_E2E/5mb_range.bin" bs=1M count=5 2>/dev/null
FID_LR=$( bsos_put "$TMPDIR_E2E/5mb_range.bin" )
dd if="$TMPDIR_E2E/5mb_range.bin" bs=1 skip=$((1024*1024)) count=4096 of="$TMPDIR_E2E/slice_exp.bin" 2>/dev/null
bsos_get -range "$((1024*1024))-$((1024*1024+4096))" "$FID_LR" "$TMPDIR_E2E/slice_got.bin"
EH=$( sha256sum "$TMPDIR_E2E/slice_exp.bin" | awk '{print $1}' )
GH=$( sha256sum "$TMPDIR_E2E/slice_got.bin" | awk '{print $1}' )
[[ "$EH" == "$GH" ]] && pass "5MB NVMe range [1MB,1MB+4096)" || fail "5MB NVMe range mismatch"

# ══════════════════════════════════════════════════════════════════════════════
section "5. Deduplication / Idempotency"

echo -n "  5.1 same content → same FID ... "
DATA="idem_$(date +%s)"
FID_A=$( echo -n "$DATA" | bsos_put - )
FID_B=$( echo -n "$DATA" | bsos_put - )
[[ "$FID_A" == "$FID_B" ]] && pass "same FID: $FID_A" || fail "different FIDs: $FID_A vs $FID_B"

echo -n "  5.2 duplicate + --no-jump → E_CONFLICT ... "
CERR=$( echo -n "$DATA" | "$BIN" put -addr "$ADDR" -no-jump - 2>&1 || true )
echo "$CERR" | grep -q "E_CONFLICT" && pass "E_CONFLICT returned" || fail "expected E_CONFLICT, got: '$CERR'"

echo -n "  5.3 different content → different FIDs ... "
FX=$( echo -n "${DATA}_X" | bsos_put - )
FY=$( echo -n "${DATA}_Y" | bsos_put - )
[[ "$FX" != "$FY" ]] && pass "different FIDs" || fail "same FID for different content: $FX"

# ══════════════════════════════════════════════════════════════════════════════
section "6. Binary Data Integrity"

echo -n "  6.1 all-zero 64 KB ... "
dd if=/dev/zero of="$TMPDIR_E2E/zeros.bin" bs=64K count=1 2>/dev/null
ORIG=$( sha256sum "$TMPDIR_E2E/zeros.bin" | awk '{print $1}' )
FZ=$( bsos_put "$TMPDIR_E2E/zeros.bin" )
bsos_get "$FZ" "$TMPDIR_E2E/zeros_ret.bin"
RET=$( sha256sum "$TMPDIR_E2E/zeros_ret.bin" | awk '{print $1}' )
[[ "$ORIG" == "$RET" ]] && pass "zero 64KB FID=$FZ" || fail "zero 64KB mismatch"

echo -n "  6.2 all-0xFF 64 KB ... "
python3 -c "import sys; sys.stdout.buffer.write(b'\xff'*65536)" > "$TMPDIR_E2E/ff.bin"
ORIG=$( sha256sum "$TMPDIR_E2E/ff.bin" | awk '{print $1}' )
FF=$( bsos_put "$TMPDIR_E2E/ff.bin" )
bsos_get "$FF" "$TMPDIR_E2E/ff_ret.bin"
RET=$( sha256sum "$TMPDIR_E2E/ff_ret.bin" | awk '{print $1}' )
[[ "$ORIG" == "$RET" ]] && pass "0xFF 64KB FID=$FF" || fail "0xFF 64KB mismatch"

echo -n "  6.3 embedded null bytes ... "
python3 -c "import sys; sys.stdout.buffer.write(b'START\x00\x00\x00MIDDLE\x00END')" > "$TMPDIR_E2E/nulls.bin"
ORIG=$( sha256sum "$TMPDIR_E2E/nulls.bin" | awk '{print $1}' )
FN=$( bsos_put "$TMPDIR_E2E/nulls.bin" )
bsos_get "$FN" "$TMPDIR_E2E/nulls_ret.bin"
RET=$( sha256sum "$TMPDIR_E2E/nulls_ret.bin" | awk '{print $1}' )
[[ "$ORIG" == "$RET" ]] && pass "null-bytes FID=$FN" || fail "null-bytes mismatch"

# ══════════════════════════════════════════════════════════════════════════════
section "7. Concurrent Writes"

echo -n "  7.1 8 parallel 256KB puts ... "
declare -a PARA_FIDS
declare -A PARA_HASH
PIDS=()
for i in $(seq 1 8); do
    F="$TMPDIR_E2E/para_${i}.bin"
    dd if=/dev/urandom of="$F" bs=256K count=1 2>/dev/null
    PARA_HASH[$i]=$( sha256sum "$F" | awk '{print $1}' )
    ( FID=$( bsos_put "$F" 2>&1 ); echo "$i $FID" > "$TMPDIR_E2E/pfid_${i}.txt" ) &
    PIDS+=($!)
done
PUTS_OK=true
for pid in "${PIDS[@]}"; do wait "$pid" || PUTS_OK=false; done
$PUTS_OK && pass "8 concurrent puts completed" || fail "concurrent put(s) failed"

echo -n "  7.2 verify all 8 parallel writes ... "
VERIFY_OK=true
for i in $(seq 1 8); do
    PFID=$( awk '{print $2}' "$TMPDIR_E2E/pfid_${i}.txt" 2>/dev/null || echo "" )
    if [[ -z "$PFID" ]]; then VERIFY_OK=false; break; fi
    bsos_get "$PFID" "$TMPDIR_E2E/para_ret_${i}.bin" 2>/dev/null || { VERIFY_OK=false; break; }
    RET=$( sha256sum "$TMPDIR_E2E/para_ret_${i}.bin" | awk '{print $1}' )
    [[ "${PARA_HASH[$i]}" != "$RET" ]] && { VERIFY_OK=false; echo -e "\n    worker $i mismatch"; break; }
done
$VERIFY_OK && pass "all 8 parallel writes verified" || fail "parallel write hash mismatch"

# ══════════════════════════════════════════════════════════════════════════════
section "8. Not-Found / Error Semantics"

echo -n "  8.1 head nonexistent FID ... "
NF=$( bsos_head 0xdeadbeefcafebabe 2>&1 || true )
echo "$NF" | grep -qi "not.found\|NotFound\|E_NOT_FOUND" && pass "head → not found" || fail "head nonexist: '$NF'"

echo -n "  8.2 get nonexistent FID ... "
NF=$( bsos_get 0xdeadbeefcafebabe - 2>&1 || true )
echo "$NF" | grep -qi "not.found\|NotFound\|E_NOT_FOUND" && pass "get → not found" || fail "get nonexist: '$NF'"

# ══════════════════════════════════════════════════════════════════════════════
section "9. JSON Output Format"

echo -n "  9.1 put -json returns valid JSON with fid ... "
JSON_PUT=$( echo -n "json_test_payload" | "$BIN" put -addr "$ADDR" -json - 2>&1 )
FID_J=$( echo "$JSON_PUT" | python3 -c "import json,sys;print(json.load(sys.stdin)['fid'])" 2>/dev/null || echo "" )
[[ -n "$FID_J" ]] && pass "put -json fid=$FID_J" || fail "put -json bad JSON: '$JSON_PUT'"

echo -n "  9.2 head -json returns valid JSON with size=17 ... "
HJ=$( bsos_head -json "$FID_J" 2>&1 )
SJ=$( echo "$HJ" | python3 -c "import json,sys;print(json.load(sys.stdin)['size'])" 2>/dev/null || echo "" )
[[ "$SJ" == "17" ]] && pass "head -json size=17" || fail "head -json size='$SJ': '$HJ'"

echo -n "  9.3 put -json returns jumps=0 for new object ... "
JT=$( echo "$JSON_PUT" | python3 -c "import json,sys;print(json.load(sys.stdin).get('jumps','MISSING'))" 2>/dev/null || echo "ERR" )
[[ "$JT" == "0" ]] && pass "put -json jumps=0" || fail "put -json jumps='$JT'"

# ══════════════════════════════════════════════════════════════════════════════
section "10. Tier Routing Boundary (small_file_pow2=8 → 1 MB)"

echo -n "  10.1 1MB-1 byte (below threshold) sha256 ... "
python3 -c "import sys; sys.stdout.buffer.write(b'Z'*(1024*1024-1))" > "$TMPDIR_E2E/almost1mb.bin"
ORIG=$( sha256sum "$TMPDIR_E2E/almost1mb.bin" | awk '{print $1}' )
FSE=$( bsos_put "$TMPDIR_E2E/almost1mb.bin" )
bsos_get "$FSE" "$TMPDIR_E2E/almost1mb_ret.bin"
RET=$( sha256sum "$TMPDIR_E2E/almost1mb_ret.bin" | awk '{print $1}' )
[[ "$ORIG" == "$RET" ]] && pass "1MB-1B sha256 match" || fail "1MB-1B mismatch"

echo -n "  10.2 exactly 1MB (at NVMe threshold) sha256 ... "
python3 -c "import sys; sys.stdout.buffer.write(b'N'*(1024*1024))" > "$TMPDIR_E2E/exactly1mb.bin"
ORIG=$( sha256sum "$TMPDIR_E2E/exactly1mb.bin" | awk '{print $1}' )
FLE=$( bsos_put "$TMPDIR_E2E/exactly1mb.bin" )
bsos_get "$FLE" "$TMPDIR_E2E/exactly1mb_ret.bin"
RET=$( sha256sum "$TMPDIR_E2E/exactly1mb_ret.bin" | awk '{print $1}' )
[[ "$ORIG" == "$RET" ]] && pass "1MB sha256 match" || fail "1MB mismatch"

# ══════════════════════════════════════════════════════════════════════════════
section "11. Unix Pipeline"

echo -n "  11.1 stdin put | xargs get stdout ... "
PAYLOAD="pipeline_$(date +%s)"
RETRIEVED=$(echo -n "$PAYLOAD" | bsos_put - | xargs -I{} "$BIN" get -addr "$ADDR" {} -)
[[ "$RETRIEVED" == "$PAYLOAD" ]] && pass "unix pipeline: put | get ✓" || fail "pipe: expected='$PAYLOAD' got='$RETRIEVED'"

echo -n "  11.2 file → stdin put → get → sha256 ... "
dd if=/dev/urandom of="$TMPDIR_E2E/pipe_src.bin" bs=128K count=1 2>/dev/null
ORIG=$( sha256sum "$TMPDIR_E2E/pipe_src.bin" | awk '{print $1}' )
FPP=$( cat "$TMPDIR_E2E/pipe_src.bin" | bsos_put - )
RET=$( bsos_get "$FPP" - | sha256sum | awk '{print $1}' )
[[ "$ORIG" == "$RET" ]] && pass "128KB stdin→stdout sha256" || fail "128KB pipe sha256 mismatch"

# ══════════════════════════════════════════════════════════════════════════════
section "12. Stress: 50 Distinct Small Objects"

echo -n "  12.1 write 50 objects and verify all ... "
STRESS_OK=true
declare -A SMAP
for i in $(seq 1 50); do
    C="stress_${i}_$(head -c 12 /dev/urandom | base64 | tr -d '=')"
    FID=$( echo -n "$C" | bsos_put - )
    SMAP[$FID]="$C"
done
for FID in "${!SMAP[@]}"; do
    G=$( bsos_get "$FID" - )
    [[ "$G" != "${SMAP[$FID]}" ]] && { STRESS_OK=false; echo -e "\n    FID $FID mismatch"; break; }
done
$STRESS_OK && pass "50 objects all verified" || fail "stress mismatch"

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════${RESET}"
TOTAL=$((PASS + FAIL))
echo -e "${BOLD}Results: ${GREEN}PASS $PASS${RESET} / ${RED}FAIL $FAIL${RESET} / Total $TOTAL${RESET}"
echo -e "${BOLD}══════════════════════════════════════════════════════${RESET}"

if [[ "${#FAIL_MSGS[@]}" -gt 0 ]]; then
    echo -e "\n${RED}${BOLD}Failed tests:${RESET}"
    for m in "${FAIL_MSGS[@]}"; do echo -e "  ${RED}•${RESET} $m"; done
fi

if [[ "$FAIL" -eq 0 ]]; then
    echo -e "\n${GREEN}${BOLD}ALL TESTS PASSED ✓${RESET}"
    exit 0
else
    echo -e "\n${RED}${BOLD}$FAIL TEST(S) FAILED${RESET}"
    exit 1
fi
