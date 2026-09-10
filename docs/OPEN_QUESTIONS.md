# Open questions

None currently open. History below for context.

## Resolved

- `total_size` vs. actual streamed byte count → abort on any mismatch,
  never silently pad/truncate. `docs/DESIGN.md` §3.3.
- Trim scheduling/locking fairness under sustained write load → accepted
  as an occasional operational risk, not engineered around. §3.8.
- Health RPC → added, same shape as NBSS's. §4.
- Higher-layer pool-lifecycle/retention system (§3.7) → explicitly a
  person, not software: an operator decides what's kept and when a pool
  retires; "GC" at that level is physical disk decommissioning. Not a
  gap, a deliberate non-automation decision — rare, consequential
  operations are exactly what shouldn't be handed to a script. §3.7.

## Reference precedent, not a target: notFinder / notFinderLinux

https://github.com/echo983/notFinder and
https://github.com/echo983/notFinderLinux are a real, shipping product in
NBSS's own ecosystem — not a BSOS target, confirmed directly. Kept on
record only as proof that this dumb-server/smart-client architecture
(typed chunk-manifest format, client-side versioning/rollback over an
immutable store) is buildable in practice. See `reference_notfinder_repos`
in project memory for detail. No action item.
