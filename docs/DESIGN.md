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
  cost. Every mechanism removed from NBSS below exists in NBSS specifically
  to pay that cost; removing it is a considered trade, not a shortcut.

## 2. Relationship to NBSS

BSOS's physical/persistence layer is carried over from NBSS essentially
unchanged: fixed-size 4KB slot grid over raw block devices, hash-derived
slot placement, per-disk read/write priority queues ordered by slot index
(elevator/SSTF-style scheduling), O_DIRECT writes with buffered reads, and
background Trim/Compact/Pack for defragmentation. See NBSS's own docs for
the mechanics; they are not repeated here except where BSOS changes their
meaning.

What BSOS changes is entirely at the *identity and request-handling*
layer: who computes an object's key, when payload data is buffered, how
collisions are handled, and the removal of DELETE/GC.

## 3. Decisions

### 3.1 Addressing

`slot = hash(client-declared key) mod slots`.

The hash is kept purely to guarantee uniform physical placement across the
data grid (avoids write hotspotting; preserves the elevator-queue and
Trim/fragmentation assumptions, which depend on writes being scattered
roughly evenly). It is *not* used to verify that a key corresponds to its
content, the way NBSS's content-derived fid does.

### 3.2 Key origin

The client computes and declares the object's key. The server never
derives an identity from content and never checks a declared key against
the bytes it receives. This is the central divergence from NBSS, where the
server always derives fid = hash(content) server-side.

### 3.3 Write path: streaming, no full-payload buffering

The client declares key + size up front (in the request header / the
first message of a streamed write). Because address placement only needs
`(key, size)` — not the payload — the server can compute the target
extent immediately, before any body bytes arrive, and then stream network
bytes straight into that extent as they arrive, in aligned chunks (reusing
the existing chunked O_DIRECT write pattern).

This removes the need for anything like NBSS's `write_memory_budget_bytes`
backpressure subsystem, which exists only because NBSS must buffer an
entire payload in RAM before it can hash it and learn where to place it.

Implementation note: this requires restructuring the write call path
(HTTP/gRPC handlers, and the low-level write function) from "accept a
fully-materialized `[]byte`" to "accept a reader/stream and write chunk by
chunk." The bytes that land on disk, and where, are unchanged — only the
plumbing between the socket and `WriteAt` changes.

### 3.4 Collision handling

If the target slot is already occupied — by anything, including a prior
write under the exact same key — the server returns a plain conflict.
There is no special case for "this looks like my own key" vs. "this is a
genuine collision," and no server-side auto-retry loop. A key that is
already registered is always a conflict, full stop.

The server does provide one primitive: a one-hop "jump pointer" — an
alias record `key A -> key B`, using the same index-entry-pair mechanism
NBSS already uses for jumpcode collision retries (an indicator entry
immediately followed by the real entry). The client decides whether to
use it and picks the target key itself; the server never auto-retries
with a server-computed candidate the way NBSS's `tryJumpWrite` does.

Only one hop is supported (no chained aliases), to keep worst-case read
cost bounded.

### 3.5 Large objects: no chunking at the storage layer

The server has no concept of splitting one logical object across multiple
keys. It is always "one key -> one contiguous byte range," exactly like
NBSS. Splitting a large logical object into multiple keys plus a manifest,
and reassembling it on read, is entirely a client-library concern.

### 3.6 Content integrity

The server performs no content validation, ever. Whether to verify is up
to the client: important data gets client-side verification (e.g. a
GET-after-write readback compare); low-stakes data doesn't need it. This
also means the server cannot distinguish a harmless retry of a client's
own prior write from a genuine key collision — see 3.4 and 3.9.

### 3.7 No DELETE, no GC

There is no DELETE in the external API. An index entry can only be
removed from the "registered" state by Trim/Compact/Pack repacking a live
small object into a new packed container and rewriting the index to point
at the new location — this is the *only* transition from registered to
unregistered. True space reclaim (deciding what is no longer wanted) is
delegated entirely to a higher layer that this system does not own. The
operational unit for reclaiming space is a whole pool (provision, fill,
retire, reinitialize), not incremental per-object deletion.

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
structure, this exclusion applies automatically: a key with an alias
pointing at it is never selected for packing, for as long as that alias
exists. The trade-off is that an aliased key permanently opts its
underlying data out of defragmentation while the alias stands; this is
expected to be a minor cost since aliasing is an occasional
collision-resolution path, not the common write path.

### 3.9 Retry-safety is a client concern, solved with an existing primitive

Because the server never validates content, it cannot tell a harmless
retry of a client's own write from a genuine collision with someone
else's write under the same key — both simply return conflict. This is
not solved server-side. A client that cares can GET the key back after a
conflict and compare against what it meant to write. Nothing new needs to
be built for this; it is the same readback tool any client already has
for its own integrity checks (3.6).

### 3.10 GET always needs the index

An earlier idea — let the client also supply size on GET, to skip the
in-memory index lookup — was considered and rejected. The in-memory
lookup is already cheap, and more importantly it is not skippable even in
principle: once Trim has repacked an object into a packed container, its
physical location is no longer a pure function of key + size — only the
index (rewritten to point at the packed table) knows where it currently
lives. GET always consults the index, unconditionally.

## 4. On-disk format

BSOS's on-disk byte format (4KB header, 16-byte index entries, data-grid
slot addressing, NBPT packed-table format) is unchanged from NBSS — no new
fields, no reinterpreted byte layout. Existing NBSS low-level tooling
(`nbss blk info`, `index-backup`/`index-restore`, `packed-scrub`) should
work unmodified against a BSOS-written disk, because nothing about the
byte grammar changes, only which subset of legal patterns is produced and
what policy decides to do on collision.

Two footnotes on top of "the format doesn't change," both semantic, not
layout, changes:

1. **Drop the jump entry's "+1 byte" payload convention.** NBSS pads a
   jump target's payload with one extra salt byte
   (`jumpData = data + jumpCode`, `jumpSize = len(data) + 1`) purely so
   that re-hashing the padded payload produces a different candidate
   address. BSOS's jump targets are client-chosen keys, not re-derived
   hashes, so there is no reason to pad the payload: a jump target's
   `actualSize` should simply equal `logicSize`, with no `+1`/`-1`
   arithmetic and no trailing-byte trim on read. This only touches the
   small piece of code that builds/reads jump entries, not the 16-byte
   entry layout.
2. **The tombstone entry pattern becomes permanently unproduced** (3.7).
   Not a layout change — the pattern remains a legal value in the format —
   just a grammar case nothing ever writes anymore. Code that still
   recognizes it is dead code, safe to delete, not required to be.

## 5. Naming

Working name: **Bare Space Object Storage (BSOS)**. "Bare" is meant
doubly: raw block device (no filesystem), and deliberately stripped of
DELETE/GC/validation/auth machinery relative to NBSS.
