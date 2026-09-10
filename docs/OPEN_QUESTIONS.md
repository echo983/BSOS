# Open questions

## Resolved (2026-09-10)

- `total_size` vs. actual streamed byte count → abort on any mismatch,
  never silently pad/truncate. See `docs/DESIGN.md` §3.3.
- Trim scheduling/locking fairness under sustained write load → accepted
  as an occasional operational risk, not engineered around. See §3.8.
- Health RPC → added, same shape as NBSS's. See §4.

## Reference precedent, not a target: notFinder / notFinderLinux

A design-closure review had flagged "no manifest format for chunking" and
"no shared client SDK" as unstarted companion systems (§3.5, and the
trust-model assumption that exactly one shared client library exists).
Both remain genuinely unstarted *for BSOS* — nothing here changes that.

What's worth recording: https://github.com/echo983/notFinder and
https://github.com/echo983/notFinderLinux are a real, shipping product in
NBSS's own ecosystem (not a BSOS target) that already proves this shape
of system is buildable: a shared `nbss-core` client crate with a typed,
versioned chunk-manifest format, plus a client-side "PVLog" layer giving
versioning/rollback/zero-copy moves purely over an immutable
content-addressed store — the same dumb-server/smart-client split BSOS is
built around. See `reference_notfinder_repos` in project memory for
detail.

BSOS does not need to be compatible with this codebase. If someone later
wants to port it onto BSOS, that's a small, bounded change, not a
redesign — noted so nobody treats it as a blocker. No action item here.

## Still open

- Higher-layer pool-lifecycle/retention system (§3.7) — still doesn't
  exist anywhere, still needed regardless of anything above.
