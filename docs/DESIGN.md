# BSOS Design (v0)

Status: design consensus reached through discussion, not yet implemented.
This document is the source of truth for that consensus; update it in
place as decisions are revised.

## 1. Scope and trust model

- Usage is bounded to a small, mutually trusted group of internal
  collaborators (on the order of Dunbar's number, roughly 150).
- The system never stores third-party ("customer") data.
- Given that scope, compliance machinery, multi-tenancy, authentication,
  and defenses against a misbehaving client are treated as accidental
  complexity, not missing requirements. They are not designed against.
  Clients are assumed to follow the protocol; if that assumption ever
  stops holding (usage grows past the trust boundary), this whole
  calculus should be revisited — that boundary condition, not a fixed
  roadmap item, is the thing to watch.
- The general principle behind every simplification below: a spec that is
  verified end-to-end (zero trust) buys the ability to coordinate with
  strangers at scale, at real, continuous verification cost. A bounded
  trust environment does not need that scale, so it should not pay that
  cost. Every mechanism removed from NBSS below exists in NBSS
  specifically to pay that cost; removing it is a considered trade, not a
  shortcut.
- Important distinction: dropping *enforcement* is not the same as
  dropping the *rule*. Where NBSS's server derives and thereby guarantees
  a property (e.g. "fid is the content's hash"), BSOS keeps the same rule
  as spec but has the client compute and declare it, and the server does
  not verify the declaration. A client that declares something
  inconsistent with the rule is out of spec; the system does not check
  for or prevent this (matching the trust model above), but the protocol
  is not designed to assume that as the common case.

## 2. Relationship to NBSS

BSOS's physical/persistence layer is carried over from NBSS essentially
unchanged: fixed-size 4KB slot grid over raw block devices, hash-derived
slot placement, O_DIRECT writes with buffered reads, and background
Trim/Compact/Pack for defragmentation. See NBSS's own docs for the
mechanics; they are not repeated here except where BSOS changes their
meaning.

One piece of NBSS's physical layer is *not* carried over: its per-disk
read/write priority queues, ordered by slot index (elevator/SSTF-style
scheduling). See §3.12 — this is dropped, not modified, given BSOS's
confirmed target medium and scale.

What BSOS changes is entirely at the *identity and request-handling*
layer: who computes an object's fid, when payload data is buffered, how
collisions are handled, the removal of DELETE/GC, the transport, and I/O
scheduling.

## 3. Decisions

### 3.1 Addressing

`slot = fid mod slots` — unchanged from NBSS's `AddrForFID`. `fid` is
still specified as `xxh3_64(content)`; the algorithm for deriving an
object's identity from its bytes does not change from NBSS. What changes
is who computes it and whether it's checked: the client computes fid and
declares it, and the server places bytes at the address that fid implies
without recomputing or verifying it against the payload.

The server performs no hashing of any kind — it treats the declared fid
purely as an address-determining value, exactly like NBSS's `AddrForFID`
already does. Because fid remains a genuine content hash for a
spec-compliant client, the placement-uniformity property NBSS gets "for
free" from hashing content carries over unchanged, with no additional
server-side mechanism required.

### 3.2 Key origin

The client computes `fid = xxh3_64(content)` and declares it, instead of
the server computing it after receiving the full payload. The *rule* that
fid is a content hash is unchanged from NBSS; only who executes and
enforces it changes (see §1's distinction between rule and enforcement).
The server never verifies a declared fid against the bytes it receives.

### 3.3 Write path: streaming, no full-payload buffering

The client declares fid + size up front (in the first message of a
gRPC client-streaming write). Because address placement only needs
`(fid, size)` — not the payload — the server can compute the target
extent immediately, before any body bytes arrive, and then stream network
bytes straight into that extent as they arrive, in aligned chunks (reusing
the existing chunked O_DIRECT write pattern).

This removes the need for anything like NBSS's `write_memory_budget_bytes`
backpressure subsystem, which exists only because NBSS must buffer an
entire payload in RAM before it can hash it and learn where to place it.
Note this composes cleanly with §3.2: a client must already hold its full
content in hand to hash it before it can declare fid, so pre-hashing
client-side costs the client nothing new — it never implies the server
needs to buffer anything.

Implementation note: this requires restructuring the write call path
(the gRPC handler, and the low-level write function) from "accept a
fully-materialized `[]byte`" to "accept a reader/stream and write chunk by
chunk." The bytes that land on disk, and where, are unchanged — only the
plumbing between the socket and `WriteAt` changes.

**Declared vs. actual byte count.** If the stream ends before `total_size`
bytes arrive, or carries more than `total_size` bytes, the server aborts
the write with an error. It never silently zero-pads a shortfall or
truncates an excess. This isn't content validation (§3.6) — the server
still never looks at *what* the bytes are — but stream-length accounting
the extent model needs to be correct at all: a silent pad on shortfall
would leave stored bytes that don't match what the client hashed to get
fid, breaking §3.1's invariant even for an honest client that hit a
transient error.

**Concurrency model: streaming writes are not atomic, and NBSS's current
locking/queueing doesn't assume that.** NBSS holds a per-disk lock for
the full duration of a write (`DeviceState.mu`), because a write is
currently a single fast `WriteAt` over already-buffered data. Under BSOS
a write can take seconds to minutes (network-bound), so holding that lock
for the whole transfer would serialize all other reads/writes to the disk
behind whichever stream happens to be in flight — an unacceptable
throughput regression, not a minor inefficiency.

The fix is a two-phase commit inside the write path, not a lock-scope
tweak. It reserves at **two levels, with two different scopes** — a
pool-wide fid-level gate, then a per-disk extent-level reservation — not
one lock doing both. Getting the fid-level gate's scope wrong (per-disk
instead of pool-wide) was tried once and found broken; see the
explanation below the steps.

0. Before anything else — before disk selection even runs — take one
   **pool-wide** lock (a single mutex covering the whole daemon, not any
   one disk) just long enough to check *and reserve, atomically*:
   - the fid-level registration for `fid` itself, against confirmed
     entries anywhere in the pool (the existing broadcast existence
     check, §3.11) and other in-flight pool-wide fid-level reservations;
   - if `alias_for` is set, the fid-level registration for `alias_for`,
     under the same check.

   If either check fails, release whatever was already reserved in this
   step and return conflict immediately — disk selection never runs.
   Otherwise both are marked pending pool-wide and the lock is released.
   This lock is held for an in-memory set lookup/insert plus the
   confirmed-existence check, fanned out as concurrent in-process calls
   against every disk's own RAM-resident index (same mechanism as
   `readAny`/`findExistingSize`, §3.11) — real work, not literally
   nothing, but neither network nor disk I/O, so still fast; never held
   at the same time as a per-disk lock (no nesting, no deadlock risk).
   Because this is now the single pool-wide choke point for every write,
   the fan-out itself needs its own short, bounded timeout, separate from
   the write-level stall timeout below — one unresponsive disk must not
   be able to freeze header processing for the whole pool.
1. Pick the target disk (§3.11's existing best-fit-by-CH_d policy, using
   `total_size`), then take *that disk's* lock just long enough to check
   *and reserve* the target data-grid extent, against confirmed occupancy
   and other in-flight extent reservations (`intervals`) on that disk —
   this is a second, disk-scoped reservation, independent of step 0's
   pool-wide one. A failure here (extremely unlikely given CH_d guided
   the choice, but possible) also returns conflict; step 0's pool-wide
   fid reservation is released again in that case.
2. Stream chunks into the reserved extent without holding either lock,
   accumulating them into slot-aligned pieces before each `WriteAt` (see
   below).
3. On a clean finish with a byte count matching `total_size`, take the
   disk lock again just long enough to write the index entry (and the
   `alias_for` pair, if set) and flip the extent reservation to
   confirmed; separately flip the pool-wide fid reservation(s) from step
   0 to confirmed too.
4. On any abort (byte-count mismatch, stream error, client
   cancellation, a reservation stall timeout — see below), release every
   reservation from steps 0 and 1 without ever writing an index entry —
   the fid(s) and the extent are free again, exactly as if the attempt
   never happened.

This pending-reservation state does not exist in NBSS today; its
occupancy tracking (`intervals`) only ever represents confirmed writes,
because nothing there is ever mid-flight. Both `computeCHD` (Bonnie's
source, §4) and ordinary conflict detection must read pending
reservations too, or they'll double-book space that's already spoken for
by an in-progress stream.

**Why fid-level reservation is a second, separate thing from extent
reservation, not covered by it.** A jump-indicator entry (what
`alias_for` produces) never touches the data grid at all — NBSS's own
format reserves no slot for a jump indicator's origin fid, only for the
real entry it points to. So a plain write to fid A racing against a
concurrent `alias_for: A` write (aliasing A to some other fid) is a
conflict that pure extent-level reservation cannot see: neither
operation's data-grid extent overlaps the other's, because the alias
operation doesn't claim an extent for A in the first place. Without a
separate fid-level reservation, both could proceed, and whichever
commits its index entry last silently wins per the log's "last entry for
a fid wins" replay rule — the other write's data would still be sitting
on disk but become permanently unreachable through that fid, with no
error raised to either caller. This can't happen in NBSS today only
because NBSS holds its lock for an entire operation, so two writes to
the same disk never interleave at all; it becomes possible the moment
that lock is shortened for streaming, which is exactly what step 0 above
is closing.

**Why that fid-level reservation has to be pool-wide, not per-disk.** An
earlier version of this fix scoped the fid-level reservation to whichever
disk the write landed on, reusing the disk's own lock instead of a
separate pool-wide one. That doesn't work: fid identity is a pool-wide
concept (existence checks already broadcast across every disk, §3.11),
but which disk a given write lands on is a heuristic decision made fresh
each time (best-fit by current CH_d). Two concurrent attempts touching
the same fid — two plain writes, or a plain write racing an
`alias_for` targeting it — can each independently pass a per-disk check
and get routed to *different* disks by that heuristic, since neither
disk's reservation state knows what the other disk is doing. The exact
silent-overwrite failure described above then reappears in a cross-disk
form that a per-disk lock structurally cannot see, no matter how
correctly it's implemented. Only a lock whose scope matches fid's actual
scope — the whole pool — closes this; that's why step 0 runs before disk
selection, not as part of it.

**Reservation stall timeout.** A client that sends `PutHeader` and then
stalls indefinitely without closing the stream would otherwise hold its
reservations until a transport-level timeout eventually notices —
typically much longer than is useful. The server enforces its own bound:
if no forward progress (first byte, or any subsequent chunk) arrives
within a configured window, the server aborts the write and releases
every reservation from steps 0 and 1 itself, rather than relying solely
on gRPC/TCP keepalive defaults.

NBSS's per-disk write priority queue (elevator/SSTF-ordered by slot
index) has a related problem one level down — see §3.12 for why it's
dropped rather than adapted to chunk granularity.

Implementation detail this implies: chunks arriving from the gRPC stream
won't be slot-aligned on their own (their size is whatever the client's
network buffering happens to produce), so the write path needs a small
internal re-alignment buffer per in-flight write that accumulates
network chunks and flushes exactly-aligned pieces to disk, rather than
passing each inbound gRPC message straight to `WriteAt`.

### 3.4 Collision handling

If the target slot is already occupied — by anything, including a prior
write under the exact same fid — the server returns a plain conflict.
There is no special case for "this looks like my own retry" vs. "a
genuine collision," and no server-side auto-retry loop. An fid that is
already registered is always a conflict, full stop.

Because fid remains spec-defined as a content hash (§3.1), the dominant
real-world reason the same fid recurs is a compliant client retrying the
same content — mirroring NBSS's idempotent-write behavior — just without
server-side verification that it's actually the same bytes this time.

The server does provide one primitive: a one-hop "jump pointer" — an
alias record `fid A -> fid B`, using the same index-entry-pair mechanism
NBSS already uses for jumpcode collision retries (an indicator entry
immediately followed by the real entry). The client decides whether to
use it and computes the target fid itself; the server never auto-retries
with a server-computed candidate the way NBSS's `tryJumpWrite` does — it
only writes whatever pair the client declares.

Only one hop is supported (no chained aliases), to keep worst-case read
cost bounded.

### 3.5 Large objects: no chunking at the storage layer

The server has no concept of splitting one logical object across multiple
fids. It is always "one fid -> one contiguous byte range," exactly like
NBSS. Splitting a large logical object into multiple fids plus a manifest,
and reassembling it on read, is entirely a client-library concern.

### 3.6 Content integrity

The server performs no content validation, ever — it never recomputes
`xxh3_64` over a received payload to check it against the declared fid.
Whether to verify is up to the client: important data gets client-side
verification (e.g. a GET-after-write readback compare); low-stakes data
doesn't need it.

### 3.7 No DELETE, no GC

There is no DELETE in the external API. An index entry can only be
removed from the "registered" state by Trim/Compact/Pack repacking a live
small object into a new packed container and rewriting the index to point
at the new location — this is the *only* transition from registered to
unregistered. True space reclaim (deciding what is no longer wanted) is
delegated entirely to a higher layer that this system does not own. The
operational unit for reclaiming space is a whole pool (provision, fill,
retire, reinitialize), not incremental per-object deletion.

That higher layer is explicitly a person, not software: an operator
decides what's still wanted and when a pool retires, on the same
operational judgment basis as any other decision this design leaves to a
human rather than automating (§1). "GC" at that level is physical —
decommissioning the disk — not a service this system calls. This is a
deliberate choice not to build tooling for something rare and consequential
enough that automating it would be the wrong instinct, not a gap to fill
later.

NBSS's tombstone entry type and everything that produces or interprets it
(the DELETE routes, the `IsTombstone` branch in index replay) has no
reason to exist in BSOS and is dropped.

### 3.8 Trim stays mandatory, and its internals are unchanged

Because Trim/Compact/Pack is now the *only* path both to reclaim data-grid
space and to keep the fixed-size append-only index stream (~256MB per
disk, same hard cap as NBSS) from filling up, it can no longer be treated
as an optional background nicety that's safe to leave disabled. It must
run continuously.

Its internal packing/candidate-selection logic does not need to change.
NBSS already excludes any fid that is a jump-alias target from packing
candidacy (`internal/daemon/fragmentation.go`: candidates are computed
from the data bucket minus the set of jump targets). Because BSOS's
client-driven one-hop alias reuses the exact same jump-bucket data
structure, this exclusion applies automatically: an fid with an alias
pointing at it is never selected for packing, for as long as that alias
exists. The trade-off is that an aliased fid permanently opts its
underlying data out of defragmentation while the alias stands; this is
expected to be a minor cost since aliasing is an occasional
collision-resolution path, not the common write path.

**No scheduling/locking fairness guarantee for Trim under sustained write
load, by decision.** Trim and ordinary reads/writes contend for the same
per-disk lock; nothing guarantees Trim gets to run if writes keep winning
that contention. If that ever starves Trim long enough for the index
stream to fill, the system stops accepting writes on that disk. This is
accepted as an occasional operational risk rather than engineered around
— not worth the complexity at this system's scale and trust model (§1).

### 3.9 Retry-safety is a client concern, solved with an existing primitive

Because the server never validates content, it cannot tell a harmless
retry of a client's own write from a genuine collision with someone
else's write under the same fid — both simply return conflict. This is
not solved server-side. A client that cares can GET the fid back after a
conflict and compare against what it meant to write. Nothing new needs to
be built for this; it is the same readback tool any client already has
for its own integrity checks (§3.6). In practice, for a spec-compliant
client this is expected to resolve correctly most of the time without
even needing the readback, since a retry of identical content produces
the identical fid by construction (§3.4).

A conflict doesn't always mean confirmed data is already there to read
back: per §3.3, it can also mean someone else's write for that fid is
merely *pending*. An immediate readback GET can legitimately come back
not-found in that case — that's not an anomaly, it's the other write
still in flight. A client's retry-safety logic should treat "conflict,
then an immediate not-found" as "try the readback again shortly," not as
an error condition.

### 3.10 GET always needs the index

An earlier idea — let the client also supply size on GET, to skip the
in-memory index lookup — was considered and rejected. The in-memory
lookup is already cheap, and more importantly it is not skippable even in
principle: once Trim has repacked an object into a packed container, its
physical location is no longer a pure function of fid + size — only the
index (rewritten to point at the packed table) knows where it currently
lives. GET always consults the index, unconditionally.

### 3.11 Multi-disk pooling carries over from NBSS unchanged

BSOS is a multi-disk pool, exactly like NBSS's `pan.json`. NBSS's own
object-identity and disk-placement decisions were already independent of
each other — `fid = hash(content)` never determined which disk an object
landed on; disk selection is a separate, server-owned best-fit decision
over each disk's CH_d (`internal/daemon/multidisk.go`:
`selectDiskForWrite`/`pickZram`/`pickNonZram`, preferring zram for small
files, otherwise the smallest CH_d disk that confidently fits, falling
back to the roomiest disk when none confidently do). Moving fid
computation to the client (§3.2) doesn't touch this axis at all, so it
carries over as-is:

- **Write**: server still picks the target disk using NBSS's existing
  best-fit-by-CH_d policy — but only after §3.3's pool-wide fid-level
  gate (step 0) has already passed. Disk selection only needs `size`,
  which is already known from `PutHeader` before any data arrives, so it
  still composes cleanly with the streaming write path; it just no
  longer runs first.
- **Read / existence-check**: server still broadcasts to all disks in
  parallel and takes the first successful answer (NBSS's `readAny`/
  `findExistingSize`), rather than maintaining a separate fid→disk
  directory — unchanged, since this was never a function of who computes
  fid either. This broadcast is what §3.3's pool-wide gate checks against
  for confirmed entries; the gate adds a pending layer on top of it, it
  doesn't replace it.
- **Alias registration**: `alias_for` (§3.4) and its target fid must land
  as an adjacent index-entry pair on the *same* disk (the jump-indicator/
  real-entry pairing is scanned per-disk at boot replay), so disk
  selection for a write with `alias_for` set picks one disk for the whole
  operation, matching how NBSS's own `tryJumpWrite` already retries
  within a single already-selected disk.

### 3.12 Target medium and scale: drop NBSS's elevator I/O queue, don't adapt it

BSOS's target medium is SSD/NVMe, not HDD, and its scale is bounded by
the trust model (§1) — an internal tool for a small, mutually trusted
group, not a system designed to absorb extreme concurrent write load
from many independent, mutually untrusted clients.

NBSS's per-disk read/write priority queue (a slot-index-ordered
min-heap, reads always draining ahead of writes) is a real, proven
technique — but its entire value comes from HDD physics: seek and
rotational latency dominate random-access cost on a spinning disk, so
reordering pending I/O by physical address measurably reduces head
movement. Flash has no seek cost; random and sequential access latency
are close enough that address-based reordering buys nothing. NVMe's own
strength — high native queue depth, many in-flight commands served in
parallel by the controller — works *against* an app-level scheme that
sorts requests into one queue and drains them in address order rather
than dispatching them concurrently.

Given that, and given the bounded scale, BSOS doesn't adapt the queue to
chunk granularity (§3.3's next-best option, superseded by this): it
drops the queue and the read-over-write priority tiering with it, and
replaces both with a simple bounded-concurrency dispatcher — a semaphore
capping how many chunk-level reads/writes are in flight at once, with no
sorting and no priority class. The cap exists only to bound resource use
under a burst, not to optimize ordering.

**Scope: one dispatcher per disk, not one global dispatcher.** Every
other piece of state this design coordinates on — the lock, `intervals`,
CH_d — is per-disk (§3.11); the dispatcher matches that. A burst of
activity on one disk must not throttle reads or writes on an unrelated
disk in the same pool.

**Accepted trade-off, named explicitly (parallel to §3.8's Trim-fairness
call): reads no longer get any latency guarantee over writes.** Reads
and writes compete equally for the same dispatcher's slots. Under
sustained write load a GET can queue behind writes for a dispatcher slot
— this is a real, deliberate consequence of dropping the priority
tiering, not an incidental side effect to discover later. It's accepted
for the same reason as §3.8: at this system's real concurrency levels a
serial drain-and-prioritize scheme was solving a problem that mostly
doesn't occur, and concurrent dispatch already gets most of what
read-priority was informally providing (nothing sits stuck behind a long
line to begin with) — but the residual risk under an actual burst is
real and is being knowingly accepted, not overlooked.

This does not change §3.3's reservation model, which stays exactly as
specified — the reservation is what keeps concurrent writes correct
(no two writes landing on overlapping slots), independent of whatever
dispatches the underlying I/O. Dropping the queue only removes an
ordering optimization that had no payoff on this medium at this scale.

## 4. Transport and wire format

**gRPC only.** BSOS does not expose an HTTP API. A client that needs HTTP
builds its own gateway/proxy in front of the gRPC service; that
translation cost is not paid by the server. This halves the server's
protocol-handling surface relative to NBSS (which maintains parallel
HTTP and gRPC handlers for every operation) and maps naturally onto
gRPC's native bidirectional/client streaming, which the write path (§3.3)
depends on.

**RPC surface: `Put`, `Get`, `Head`, `Bonnie`, `Health`.** Settled by elimination
from NBSS's HTTP+gRPC surface:

- **`Delete`**: dropped, per §3.7 (no DELETE/GC).
- **`Probe`**: dropped entirely, including the narrower "batch existence
  check" use case that survived the first cut. Reasoning: NBSS's PROBE
  existed to avoid paying for a full payload upload just to learn about a
  collision; BSOS's `PutHeader`-first write path already fails at the
  header, before any payload moves, so PUT itself is already nearly as
  cheap as a pure check. A dedicated batch-check RPC doesn't clear the bar
  on top of that: per-item `Head` already covers "check without writing,"
  and gRPC/HTTP2 already multiplexes many concurrent per-item calls over
  one connection cheaply, so there's no real round-trip cost left for a
  bespoke batch message format to save.

  This "fails at the header" property has two implementation
  preconditions that don't happen automatically and are binding on both
  sides, not just a server-side claim: the server must close the RPC
  with an error status the moment it detects a conflict while processing
  `PutHeader`, rather than waiting for the client to finish sending; and
  the client SDK must check for a stream error after every `chunk` send
  and stop sending on one, rather than queueing the whole payload without
  checking. A client that blasts every chunk without checking gets no
  benefit from the header-first design even though the server is doing
  its part correctly.
- **`Pan`**: dropped. Disk topology, per-disk CH_d, and placement policy
  are server-internal plumbing (§3.11); external clients have no reason
  to see them and get everything they need through `Bonnie`'s single
  aggregated number.
- **`Head`**: kept. Cheap existence/size check without paying for a
  payload transfer — useful standalone, and as the tool a client can use
  for the readback check in §3.9.
- **`Bonnie`**: kept, but reshaped. `write_backpressure_hint` is dropped
  — it reported pressure on NBSS's `write_memory_budget_bytes`
  subsystem, which BSOS doesn't have (§3.3). `ch_d_pow2` is *kept*: it's
  a pure function of current occupancy (`internal/daemon/state.go:
  computeCHD` samples 64 candidate fids and binary-searches for the
  largest extent size placeable with the configured target probability),
  independent of who computes fid or how payload buffering works. Its
  role is repurposed as the signal a client uses to size chunks when
  splitting a large object (§3.5): `ch_d_pow2` is the client's best
  available estimate of "how big a single fid can I expect to place
  successfully right now," and it visibly degrades if Trim (§3.8) isn't
  keeping up. Multi-disk aggregation reuses NBSS's existing
  `bonnieCHDPow2()` policy unchanged: max CH_d across writable non-zram
  disks, capped by `max_put_bytes`.
- **`Health`**: kept, same shape as NBSS's. Operational infrastructure
  (liveness/readiness for a load balancer or monitor to poll), not a data
  operation — orthogonal to every trust-model/identity decision above.

Draft proto shape:

```protobuf
message PutHeader {
  uint64 fid        = 1;  // client-computed xxh3_64(content), per §3.1/3.2
  uint64 total_size = 2;  // required; must arrive before any data chunk
  uint64 alias_for  = 3;  // optional; 0 = normal put. Non-zero: also
                           // register alias_for -> fid as a one-hop jump
                           // (§3.4), in the same write.
}

message PutRequest {
  oneof msg {
    PutHeader header = 1;  // must be the first message on the stream
    bytes     chunk  = 2;  // subsequent messages; written straight to disk
  }
}

message PutResponse {}     // success is empty; conflict is a gRPC status
                            // (e.g. AlreadyExists), not a response field

message GetRequest {
  uint64 fid         = 1;
  bool   has_range   = 2;
  uint64 range_start = 3;
  uint64 range_end   = 4;
}

message GetResponse {
  uint64 size        = 1;
  uint64 range_start = 2;
  uint64 range_end   = 3;
  bytes  data        = 4;
}

message HeadRequest {
  uint64 fid = 1;
}

message HeadResponse {
  uint64 size = 1;
}

message BonnieResponse {
  uint32 ch_d_pow2 = 1;  // largest object size placeable with the
                          // configured target probability, right now
}

message Empty {}

message HealthResponse {
  bool ok = 1;
}
```

`PutHeader.alias_for` folds jump-pointer registration into the same
write call rather than a separate RPC, mirroring NBSS's own
`tryJumpWrite`, which writes the jump-indicator and the real data entry
as one operation.

## 5. On-disk format

BSOS's on-disk byte format (4KB header, 16-byte index entries, data-grid
slot addressing, NBPT packed-table format) is unchanged from NBSS — no new
fields, no reinterpreted byte layout. Existing NBSS low-level tooling
(`nbss blk info`, `index-backup`/`index-restore`, `packed-scrub`) should
work unmodified against a BSOS-written disk, because nothing about the
byte grammar changes, only which subset of legal patterns is produced and
what policy decides to do on collision.

One footnote on top of "the format doesn't change" — semantic, not
layout:

1. **Keep NBSS's jump-entry "+1 byte" payload convention as the
   recommended client-side algorithm**, rather than treating it as
   obsolete. NBSS pads a jump target's payload with one extra salt byte
   (`jumpData = data + jumpCode`, `jumpSize = len(data) + 1`,
   `jumpFID = xxh3_64(jumpData)`) so that re-hashing the padded payload
   produces a new candidate fid. Since fid remains spec-defined as a
   content hash (§3.1), this is still the natural, spec-consistent way
   for a client to pick a jump target on collision, and the client SDK
   should implement it exactly as NBSS's `tryJumpWrite` does. The server
   is agnostic to how a client derives `alias_for`'s target fid — it only
   writes whatever pair is declared — but the recommended algorithm
   carries over unchanged, including `actualSize = logicSize + 1` and the
   trailing-byte trim on read.

The tombstone entry pattern (§3.7) also becomes permanently unproduced,
but that's a §3.7 consequence, not a format concern of its own: the
pattern remains legal in the format, nothing ever writes it anymore.

## 6. Naming

Working name: **Bare Space Object Storage (BSOS)**. "Bare" is meant
doubly: raw block device (no filesystem), and deliberately stripped of
DELETE/GC/validation/auth machinery relative to NBSS.
