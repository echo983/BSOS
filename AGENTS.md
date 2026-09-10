# BSOS development

- Consult `docs/IMPLEMENTATION_PLAN.md` and the latest validation report before
  continuing a milestone; distinguish simulated tests from real-device gates.
- Default checks: `go build ./...`, `go vet ./...`,
  `go test -race ./... -count=1`, and `git diff --check`.
- Real device tests are opt-in and may append persistent objects. Do not enable
  them against unrelated disks.
- A dedicated Debian VPS is authorized by the user for BSOS deployments,
  zram testing, and other needed integration work. The connection information
  and authorization record are in ignored `config/test-host.local.md`.
  Read that record before remote work; no SSH private key should be copied.
- Do not commit private host coordinates or test credentials. Record public
  reproducible procedures and results in `docs/`, with host-specific details
  only in the ignored local configuration.
