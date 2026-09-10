#!/usr/bin/env bash
# BSOS Shell Pipeline Automation Example
# Demonstrates streaming data through Unix pipelines using the 'bsos' CLI.
set -euo pipefail

ADDR="${BSOS_ADDR:-127.0.0.1:9090}"
echo "[*] Using BSOS Daemon Address: ${ADDR}"

# 1. Health check
echo -n "[*] Checking cluster health: "
bsos health -addr "${ADDR}"

# 2. Check cluster allocation capacity with Bonnie
echo "[*] Querying Bonnie capacity limit:"
bsos bonnie -addr "${ADDR}" -json

# 3. Stream a generated tarball archive directly to BSOS via stdin
TMPDIR="$(mktemp -d -t bsos-pipe-XXXXXX)"
trap 'rm -rf "${TMPDIR}"' EXIT

mkdir -p "${TMPDIR}/sample_dataset"
echo "Document 1: Antigravity Bare Space Object Storage" > "${TMPDIR}/sample_dataset/doc1.txt"
echo "Document 2: Deterministic extent allocation with zero file system overhead" > "${TMPDIR}/sample_dataset/doc2.txt"

echo "[*] Creating tarball and piping to 'bsos put -json -'..."
PUT_OUTPUT=$(tar -C "${TMPDIR}" -czf - sample_dataset | bsos put -addr "${ADDR}" -json -)
echo "[+] Put Result: ${PUT_OUTPUT}"

# Extract FID from JSON output (using jq if available, else python/grep fallback)
if command -v jq >/dev/null 2>&1; then
    FID=$(echo "${PUT_OUTPUT}" | jq -r .fid)
    SIZE=$(echo "${PUT_OUTPUT}" | jq -r .size)
else
    FID=$(echo "${PUT_OUTPUT}" | grep -o '"fid":"[^"]*"' | cut -d'"' -f4)
    SIZE=$(echo "${PUT_OUTPUT}" | grep -o '"size":[0-9]*' | cut -d':' -f2)
fi

echo "[+] Object stored with FID: ${FID} (${SIZE} bytes)"

# 4. Check object metadata using 'bsos head'
echo "[*] Running Head check on object ${FID}:"
bsos head -addr "${ADDR}" -json "${FID}"

# 5. Stream object back through tar extraction without intermediate archive file
RESTORE_DIR="${TMPDIR}/restored"
mkdir -p "${RESTORE_DIR}"
echo "[*] Streaming archive back from BSOS via stdout to 'tar -x':"
bsos get -addr "${ADDR}" "${FID}" - | tar -C "${RESTORE_DIR}" -xzf -

echo "[+] Verifying restored contents:"
cat "${RESTORE_DIR}/sample_dataset/doc1.txt"
cat "${RESTORE_DIR}/sample_dataset/doc2.txt"

# 6. Perform a partial range read on the object (first 16 bytes)
echo -n "[*] Range read [0..16) header bytes: "
bsos get -addr "${ADDR}" -range 0-16 "${FID}" | xxd -p || true

echo ""
echo "[✓] Pipeline demonstration completed successfully!"
