# O_DIRECT write path — real validation (2026-09-11)

Status: `docs/DESIGN.md` §3.3 requires writes to use O_DIRECT (reads stay
buffered). Until this date the implementation never wired this in at all —
`internal/daemon/state.go` opened every device with a plain buffered fd,
flagged as a known gap in `docs/FOUNDATION_VALIDATION_2026-09-10.md`. This
doc records closing that gap: the new `internal/daemon/directio.go`
streaming aligned-writer, its unit tests, and real validation on the
authorized test VPS (`config/test-host.local.md`). See
`docs/REMOTE_E2E_VALIDATION_2026-09-10.md` for a correction note — that
doc's Phase 3 predates this work and its "Direct I/O" phrasing was not yet
accurate at the time it was written.

Scope decisions (already made before implementation): O_DIRECT covers only
the main object-data write path (`PreparedWrite.WriteFromContext`); Trim/
Compact's container-repacking writes stay buffered, out of scope. If
O_DIRECT is unavailable on a host/filesystem, the daemon logs a warning and
falls back to buffered writes rather than failing to start.

## 1. Local unit tests (`internal/daemon/directio_test.go`)

Pure algorithm tests (`writeStreamDirect` against a fake in-memory
`io.WriterAt`, no real O_DIRECT open needed): alignment invariant (every
`WriteAt`'s offset and length are multiples of `blk.SlotSize`) and content
correctness (real bytes + zero pad reconstruct exactly), across object
sizes from 1 byte up to multiple full `directChunkBytes` (1 MiB) chunks
plus a remainder, oddly-sized stream delivery
(`testing/iotest.OneByteReader`/`HalfReader`), and both error paths (short
stream, over-long stream). All pass.

Fallback/wiring tests (package-level `openDirectFile` var swapped for a
stub): confirmed `OpenDevice` never fails to start when O_DIRECT open
fails, logs `"O_DIRECT unavailable for %s, falling back to buffered
writes"`, and a full `PrepareWrite`→`WriteFromContext`→`Commit`→`Get` round
trip on a non-slot-aligned object size still produces byte-correct results
via the buffered fallback branch. A second test with a working stub
confirms `WriteFromContext` actually routes through `writeStreamDirect`
when `directFile` is set.

Unexpectedly, this dev sandbox's `/tmp` is tmpfs on a 6.6.141 kernel — new
enough to support O_DIRECT on tmpfs (`shmem` direct I/O landed in Linux
6.6) — so the unmocked `TestOpenDeviceRealDirectOpenBestEffort` case
actually exercised real O_DIRECT locally too, not just via the stub.

`go test -race ./... -count=1` (full repo): all packages pass.
`go vet ./...`, `gofmt -l .`: clean. `go mod tidy` promoted
`golang.org/x/sys` from an indirect to a direct dependency.

## 2. VPS filesystem probe (Step 0)

`debian@62.171.177.64` (`vmi3340548`, Debian 13, kernel
`6.12.38+deb13-cloud-amd64`). `/var/lib/bsos-test` is `ext4` on `/dev/sda1`.
`dd if=/dev/zero of=<probe file> bs=4096 count=1 oflag=direct` exited 0 —
O_DIRECT works on this filesystem, no `fallocate`-vs-`truncate` workaround
needed for `scripts/test-vps-setup.sh`.

**Incident during this probe**: the initial `dd oflag=direct` command was
run directly against the live `disk.img` (a mistake — should have used a
disposable scratch file), zeroing its 4096-byte header (magic, version,
disk_id). The ~256 MB index stream region was never touched. Repaired by
reconstructing the exact original header bytes (`magic="NBSS"`,
`version=2`, `capacity_gb=0`, `disk_id=0x42534F530001`, matching
`internal/blk/header.go`'s `buildHeader` layout and the values in
`/var/lib/bsos-test/pan.json`) and writing them back directly, without
touching the index or data-grid regions. Verified via `bsos blk info`
(old binary, before redeploying) — output matched the original values
exactly — and via the still-running old daemon's `Health` RPC (unaffected
throughout, since it had the file open from before the corruption).

## 3. Deployment

Cross-compiled `bin/bsosd`/`bin/bsos` (`GOOS=linux GOARCH=amd64`), copied
to the VPS, reinstalled to `/usr/local/lib/bsos-test/`, restarted
`bsos-test.service`. No config changes (no `direct_io` toggle exists by
design — see `docs/DESIGN.md` §3.3, "mandatory, no on/off switch" pattern
matching Trim).

## 4. Positive confirmation O_DIRECT was actually used

A functional Put/Get pass alone doesn't prove this (the fallback path is
silently correct too), so two independent checks were used:

**Log-based**: startup log showed one success line per pan device:

```
bsosd: opened /var/lib/bsos-test/disk.img with O_DIRECT for the write path
bsosd: opened /dev/zram0 with O_DIRECT for the write path
bsosd: listening on :19090 (2 disk(s))
```

**Kernel-level, independent of anything the daemon printed**: read
`/proc/<pid>/fdinfo/<fd>` for both fds each pan device holds (`file`, the
buffered fd, and `directFile`, opened second). For `disk.img`:

| fd | flags (octal) | flags (decimal) |
|----|----------------|------------------|
| 4 (buffered `file`) | `02100002` | 557058 |
| 7 (`directFile`) | `02140002` | 573442 |

`573442 − 557058 = 16384 = 0x4000` — exactly the `O_DIRECT` bit
(`00040000` octal per `asm/fcntl.h` on x86_64), and no other bit differs
between the two fds. The same relationship held for `/dev/zram0`'s pair
(fd 8 buffered, fd 9 direct, same flag delta). This confirms, from the
kernel's own record of how each fd was opened, that `directFile` really is
O_DIRECT and `file` really is not — for both pan devices.

## 5. Functional + alignment-stress correctness under real O_DIRECT

Ran the existing `scripts/remote-e2e/main.go` suite against the redeployed
VPS, extended with a new **Phase 3b**: a `2*directChunkBytes + 12345` byte
(~2 MiB + 12 KB) object, deliberately sized to straddle a `directChunkBytes`
(1 MiB) boundary with a non-trivial remainder — the existing Phase 3
object (6 MiB, exactly six 1 MiB chunks) happens to land on clean chunk
boundaries, the least interesting case for the new aligned-chunk writer.

All phases passed, including the new one:

```
==> Phase 3: Large Streaming Object (6 MiB in 1 MiB chunks across WAN)...
  [PASS] Streamed 6 MiB Put to remote VPS in 840.059002ms (7.14 MB/s, jumps=4)
  [PASS] Streamed 6 MiB Get from remote VPS in 812.525593ms (7.38 MB/s), SHA-256 matched perfectly
==> Phase 3b: O_DIRECT Alignment-Stress Object (2 MiB + 12345 B, non-chunk-aligned)...
  [PASS] 2109497 B object (2 chunks + non-aligned remainder) round-tripped through real O_DIRECT, SHA-256 matched perfectly
...
ALL LOCAL-TO-REMOTE END-TO-END TESTS PASSED WITH 100% SUCCESS!
```

(Full output covers Phases 1-6 plus the CLI checks, unchanged from the
2026-09-10 run's coverage, now additionally running over real O_DIRECT on
both pan devices rather than the buffered path that existed at that date.)

## 6. Conclusion

O_DIRECT is real on the write path, verified independently at the log
level, the kernel fd-flags level, and functionally (including a
deliberately non-chunk-aligned object) over a real public-internet WAN
connection to the authorized test VPS. Reads remain buffered, unchanged.
The fallback-when-unsupported behavior is also verified, locally, via the
dedicated unit tests in §1. `docs/DESIGN.md` §3.3's existing line about
"reusing the existing chunked O_DIRECT write pattern" is now literally
true and needed no edit.
