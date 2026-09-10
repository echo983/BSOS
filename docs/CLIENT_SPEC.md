# Client behavior specification

Status: v0, alongside `docs/IMPLEMENTATION_PLAN.md`. This document
specifies what any BSOS client — the Go library and CLI this repo
builds, and any other implementation — must do to be spec-compliant. It
is a client-facing contract; `docs/DESIGN.md` explains and justifies the
server side. Where the two overlap, `docs/DESIGN.md` is authoritative on
*why*, this document is authoritative on *what a client does*.

## Scope boundary: BSOS is infrastructure, not an application layer

BSOS has no concept of files, directories, paths, versions, or any
structure above "a byte string identified by its content hash." A
client — including the Go library and CLI in this repo — must not
invent one. In particular:

- **No chunking or manifest support of any kind.** Splitting a large
  logical object into multiple BSOS objects, and any format for
  recording how to reassemble them, is an application-layer concern
  entirely outside this project (`docs/DESIGN.md` §3.5). This is not
  deferred pending a future manifest-format design — it is out of scope,
  permanently, for the same reason the server doesn't own it: BSOS
  doesn't know what a "file" is, and isn't the layer that should decide.
- Bonnie's `ch_d_pow2` (§2.5 below) is documented here anyway, because a
  *higher* layer that does implement chunking will need it — but this
  document only specifies how to interpret the number, not how to use it
  to build a manifest.

## 1. Identity

`fid = xxh3_64(content)`, computed over the exact bytes being written.
The server never verifies this (`docs/DESIGN.md` §3.1/§3.2/§3.6); a
client that declares an fid inconsistent with its content is out of
spec, and nothing detects or prevents this. A client must have the
complete content in hand before it can compute fid and start a `Put`.

## 2. Write protocol (`Put`)

1. Send `PutHeader{fid, total_size, alias_for}` as the *first* message on
   the stream. `alias_for` is `0` for a normal write; non-zero registers
   `alias_for -> fid` as a one-hop jump pointer in the same write (§3.4).
2. Send the payload as subsequent `chunk` messages, in order, totaling
   exactly `total_size` bytes. Any other total is an error on the
   server's side (`docs/DESIGN.md` §3.3) — get the count right.
3. **Check for a stream/send error after every chunk send, and stop
   sending immediately if one occurs.** This is not optional: the
   server's "fails at the header, before wasting bandwidth" behavior
   (§4) only holds if the client is actually watching for the server's
   early rejection instead of blindly queueing the whole payload. A
   client that ignores send errors gets none of that benefit even though
   the server did its part correctly.
4. On success, `PutResponse` is empty. On conflict, the RPC ends with a
   gRPC error status (e.g. `AlreadyExists`) — see §3.

## 3. Collision handling

A conflict means the fid (or, when set, `alias_for`) is already
registered — confirmed, or another write for it is merely in flight
(§3.9). There is no server-side retry. A client that wants to retry has
two supported paths:

- **Give up / treat as done.** Reasonable when a conflict most likely
  means "this exact content is already there" (§3.4) — true whenever the
  client is retrying its own prior write of identical bytes, since
  identical content always produces the identical fid.
- **Retry via a one-hop alias.** The recommended algorithm, matching
  NBSS's own `tryJumpWrite` and kept for the reason given in
  `docs/DESIGN.md` §5's footnote (fid remains a content hash, so this is
  still the spec-consistent way to pick a new address):

  ```
  for jumpCode in 1..255:
    jumpData = content + byte(jumpCode)
    jumpFID  = xxh3_64(jumpData)
    try Put(PutHeader{fid: jumpFID, total_size: len(jumpData), alias_for: fid})
    if it succeeds: done, the object is reachable via `fid` (one hop) or
      directly via `jumpFID`
    if conflict: try the next jumpCode
  ```

  Give up after exhausting the range (or a smaller client-chosen limit)
  and surface the failure — this is expected to be exceedingly rare in
  practice.

## 4. Retry-safety

Because the server never validates content, a conflict on retry doesn't
by itself prove the data present is identical, and a readback
immediately after a conflict can legitimately return not-found if the
other write is still pending (`docs/DESIGN.md` §3.9). A client that needs
certainty:

1. `Get` (or `Head`, if only size needs checking) the fid.
2. If not-found, wait briefly and retry the readback — this is normal,
   not an error, while another write for the same fid is in flight.
3. If found, compare against the content the client meant to write.

Low-stakes data can skip this entirely and simply treat any conflict as
success (§3.6's "important data gets verification, low-stakes data
doesn't" framing).

## 5. Interpreting `Bonnie`

`ch_d_pow2` is the largest object size the pool can currently place with
its configured target success probability (`docs/DESIGN.md` §4). A
caller can treat `2^ch_d_pow2` bytes as a rough ceiling for a single
`Put`'s size before placement becomes noticeably less reliable. This
document stops there — turning that into an actual chunk size, chunk
count, and reassembly plan for a logical object larger than that ceiling
is exactly the application-layer job this project doesn't do (see the
scope boundary above).

## 6. Errors

- `AlreadyExists` (or equivalent): conflict, §3.
- `NotFound`: `Get`/`Head` on an fid with no confirmed entry.
- Any error while sending: stop sending immediately (§2.3).
- `ResourceExhausted` or similar: the server's own bounded-concurrency
  dispatcher (`docs/DESIGN.md` §3.12) or a reservation-related timeout
  rejected the attempt — safe to retry the whole `Put` from scratch.

## Current transport details (2026-09-10)

Get responses carry at most 1 MiB of payload per message. Concatenate their
`data` fields in order; `size` is the complete logical object size, and
`range_start`/`range_end` describe each response's half-open byte interval.
For an empty requested range, the server sends one empty response with the
range metadata. An omitted range reads the complete object. With a range,
`range_end = 0` means object end; ends beyond the object are clamped, and
starts beyond the resulting end are rejected.

The current direct/alias storage profile requires nonempty objects;
`total_size = 0`, a self-alias, and an alias payload shorter than two bytes
are rejected before reservation. A write response failure after commit
leaves the object confirmed: use the existing readback retry-safety rule.
