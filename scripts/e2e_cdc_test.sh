#!/usr/bin/env bash
# scripts/e2e_cdc_test.sh - End-to-End Test for FastCDC Content-Defined Chunking & Dual-Path Client
set -euo pipefail

BSOS_BIN="./bsos"
TARGET_ADDR="${BSOS_ADDR:-192.168.1.79:9090}"
TMP_DIR=$(mktemp -d /tmp/bsos_cdc_test_XXXXXX)
trap 'rm -rf "${TMP_DIR}"' EXIT

json_get() {
    python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('$1', ''))"
}

echo "================================================================="
echo " BSOS Dual-Path & FastCDC E2E Verification Suite"
echo " Target Daemon: ${TARGET_ADDR}"
echo " Temporary Workspace: ${TMP_DIR}"
echo "================================================================="

# Check Daemon Health
echo "[1/6] Checking cluster health at ${TARGET_ADDR}..."
"${BSOS_BIN}" health -addr "${TARGET_ADDR}"

# 1. Atomic Upload Test (Small file: 4 MB)
echo "[2/6] Testing Dual-Path Atomic Upload (4MB)..."
SMALL_SRC="${TMP_DIR}/small_4mb.bin"
SMALL_DST="${TMP_DIR}/small_4mb_out.bin"
dd if=/dev/urandom of="${SMALL_SRC}" bs=1M count=4 status=none
ORIG_SMALL_SHA=$(sha256sum "${SMALL_SRC}" | awk '{print $1}')

ATOMIC_PUT_JSON=$("${BSOS_BIN}" put -addr "${TARGET_ADDR}" -json "${SMALL_SRC}")
SMALL_FID=$(echo "${ATOMIC_PUT_JSON}" | json_get "fid")
UPLOAD_TYPE=$(echo "${ATOMIC_PUT_JSON}" | json_get "type")

if [ "${UPLOAD_TYPE}" != "atomic" ]; then
    echo "FAILED: Expected upload type 'atomic', got '${UPLOAD_TYPE}'"
    exit 1
fi
echo "  -> Uploaded as atomic object: FID = ${SMALL_FID}"

"${BSOS_BIN}" get -addr "${TARGET_ADDR}" "${SMALL_FID}" "${SMALL_DST}"
GET_SMALL_SHA=$(sha256sum "${SMALL_DST}" | awk '{print $1}')
if [ "${ORIG_SMALL_SHA}" != "${GET_SMALL_SHA}" ]; then
    echo "FAILED: SHA-256 mismatch on atomic get"
    exit 1
fi
echo "  -> Atomic Get SHA-256 match verified: ${GET_SMALL_SHA}"

# 2. FastCDC Chunking Upload Test (Large file: 64 MB)
echo "[3/6] Testing FastCDC Chunked Upload (64MB)..."
LARGE_SRC="${TMP_DIR}/large_64mb.bin"
LARGE_DST="${TMP_DIR}/large_64mb_out.bin"
dd if=/dev/urandom of="${LARGE_SRC}" bs=1M count=64 status=none
ORIG_LARGE_SHA=$(sha256sum "${LARGE_SRC}" | awk '{print $1}')

CDC_PUT_JSON=$("${BSOS_BIN}" put -addr "${TARGET_ADDR}" -json "${LARGE_SRC}")
MANIFEST_FID=$(echo "${CDC_PUT_JSON}" | json_get "manifest_fid")
CDC_TYPE=$(echo "${CDC_PUT_JSON}" | json_get "type")
CHUNK_COUNT=$(echo "${CDC_PUT_JSON}" | json_get "chunks")
UPLOADED_COUNT=$(echo "${CDC_PUT_JSON}" | json_get "uploaded")

if [ "${CDC_TYPE}" != "cdc" ]; then
    echo "FAILED: Expected upload type 'cdc', got '${CDC_TYPE}'"
    exit 1
fi
echo "  -> Uploaded as CDC Manifest: FID = ${MANIFEST_FID}"
echo "  -> Total Chunks: ${CHUNK_COUNT}, Uploaded: ${UPLOADED_COUNT}"

# 3. Manifest Inspect Test
echo "[4/6] Inspecting Manifest metadata..."
"${BSOS_BIN}" manifest inspect -addr "${TARGET_ADDR}" "${MANIFEST_FID}"

# 4. Transparent GetAuto & Sparse Range Read Test
echo "[5/6] Testing Transparent Get & Sparse Range Reads..."
# Reassemble full file
"${BSOS_BIN}" get -addr "${TARGET_ADDR}" "${MANIFEST_FID}" "${LARGE_DST}"
GET_LARGE_SHA=$(sha256sum "${LARGE_DST}" | awk '{print $1}')
if [ "${ORIG_LARGE_SHA}" != "${GET_LARGE_SHA}" ]; then
    echo "FAILED: SHA-256 mismatch on CDC reassembly"
    exit 1
fi
echo "  -> CDC Full Reassembly SHA-256 match verified: ${GET_LARGE_SHA}"

# Test Sparse Range Read [10MB, 25MB)
RANGE_START=$((10 * 1024 * 1024))
RANGE_END=$((25 * 1024 * 1024))
RANGE_DST="${TMP_DIR}/range_10_25mb.bin"
"${BSOS_BIN}" get -addr "${TARGET_ADDR}" -range "${RANGE_START}-${RANGE_END}" "${MANIFEST_FID}" "${RANGE_DST}"

# Verify slice directly against source file
EXPECTED_SLICE="${TMP_DIR}/expected_slice.bin"
dd if="${LARGE_SRC}" of="${EXPECTED_SLICE}" bs=1M skip=10 count=15 status=none
SLICE_SHA=$(sha256sum "${RANGE_DST}" | awk '{print $1}')
EXPECTED_SHA=$(sha256sum "${EXPECTED_SLICE}" | awk '{print $1}')

if [ "${SLICE_SHA}" != "${EXPECTED_SHA}" ]; then
    echo "FAILED: Sparse range read SHA-256 mismatch"
    exit 1
fi
echo "  -> Sparse Range Read [10MB-25MB) verified: ${SLICE_SHA}"

# 5. Boundary-Shift & Content Mutation Deduplication Test
echo "[6/6] Testing Incremental Deduplication on Content Mutation..."
MUTATED_SRC="${TMP_DIR}/mutated_64mb.bin"
cp "${LARGE_SRC}" "${MUTATED_SRC}"
# Mutate 64KB at offset 30MB
dd if=/dev/urandom of="${MUTATED_SRC}" bs=64k count=1 seek=480 conv=notrunc status=none

MUTATED_PUT_JSON=$("${BSOS_BIN}" put -addr "${TARGET_ADDR}" -json "${MUTATED_SRC}")
MUT_MANIFEST_FID=$(echo "${MUTATED_PUT_JSON}" | json_get "manifest_fid")
MUT_CHUNKS=$(echo "${MUTATED_PUT_JSON}" | json_get "chunks")
MUT_DEDUP=$(echo "${MUTATED_PUT_JSON}" | json_get "deduplicated")
MUT_UPLOADED=$(echo "${MUTATED_PUT_JSON}" | json_get "uploaded")

echo "  -> Mutated Manifest FID: ${MUT_MANIFEST_FID}"
echo "  -> Total Chunks: ${MUT_CHUNKS}, Deduplicated: ${MUT_DEDUP}, Re-uploaded: ${MUT_UPLOADED}"

if [ "${MUT_DEDUP}" -eq 0 ]; then
    echo "FAILED: Expected deduplicated chunks > 0, got 0"
    exit 1
fi

echo "================================================================="
echo " ALL E2E DUAL-PATH & FASTCDC TESTS PASSED SUCCESSFULLY!"
echo "================================================================="
