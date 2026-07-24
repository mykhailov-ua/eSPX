# eSPX Installer Runbook

`espx-install` provisions and configures an on-prem eSPX deployment.

## Quick start

```bash
go build -o espx-install ./cmd/installer

./espx-install preflight --strict
./espx-install provision --yes
./espx-install configure --interactive
./espx-install apply --dry-run
sudo ./espx-install apply
./espx-install doctor
```

## Commands

| Command | Purpose |
| :--- | :--- |
| `preflight [--strict] [--json]` | PF-KERNEL, PF-BTF, PF-NIC, PF-LIBS, PF-PORTS, PF-ULIMIT, PF-SYSCTL |
| `provision [--yes]` | Install OS packages from `deploy/installer/packages.yaml` (no `dist-upgrade`) |
| `configure [--interactive]` | Write `install.yaml` (profile + feature flags) |
| `apply [--dry-run]` | Render systemd units, secrets, compose env; optional binary deploy with health probe + rollback |
| `rollback <tracker\|processor>` | Restore last backup binary after failed deploy or crash loop |
| `doctor [--json]` | Run `scripts/ci/check_deps.sh` and topology probes |
| `license install\|activate\|status` | Product license lifecycle (M3) |

## Profiles

- `single_vps` — bare-metal / single VPS with systemd units
- `compose_dev` — local compose stack via `scripts/local-dev/dev_stack.sh`
- `k8s_k3s` — k3s install via `scripts/k8s/install_k3s.sh` (requires cgroup v2)

Feature flags:

- `edge_xdp` — requires M5 edge XDP and PF-BTF
- `multi_region` — Enterprise multi-cell topology (M7); requires JWT `multi_region` + `MULTI_REGION_ENABLED=1`
- `telemetry_enabled` — defaults off in production profiles
- `ingress_schema` — `openrtb_3` (default) or `espx_native` (legacy TrackRequest/AdEvent)
- `tracker` / `processor` — optional `binary`, `health_url`, `version` for rollback-safe deploy

## License

```bash
# Air-gap install from vendor JWT file
ESPX_LICENSE_SRC=/path/to/license.jwt ./espx-install license install

# Online activation
ESPX_LICENSE_SERVER=https://license.example.com \
ESPX_LICENSE_KEY=... \
ESPX_DEPLOYMENT_ID=... \
./espx-install license activate

./espx-install license status
```

## Files

| Path | Role |
| :--- | :--- |
| `install.yaml` | Operator profile written by `configure` |
| `/etc/espx/secrets.env` | Generated secrets (mode 600) |
| `/etc/espx/license.jwt` | Product license JWT |
| `deploy/installer/packages.yaml` | Debian package manifest |

## Testing

```bash
go test ./internal/installer/... -short
go test ./internal/installer/... -run Preflight -short
```

Integration `apply` on a VM is manual and does not block PRs.
