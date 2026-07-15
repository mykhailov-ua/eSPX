# Edge ingress hardening

Artifacts for the public ingress node (tracker `:8180`). Two layers: host tuning (Phase 0) and optional XDP filtering.

## Phase 0 — host tuning

Applied on bare metal via scripts (not in default compose).

| File | Role |
|------|------|
| `99-espx-edge.conf` | sysctl: `somaxconn`, TCP buffers, SYN cookies |
| `nic-tune.service` | systemd oneshot: RX ring + IRQ spread |
| `nic-tune.env.example` | `INGRESS_INTERFACE`, `IRQ_STRATEGY` |

```bash
sudo bash scripts/edge-tuning/edge_sysctl.sh apply
sudo bash scripts/edge-tuning/edge_nic_tune.sh install-systemd
sudo systemctl start espx-edge-nic-tune
```

Or run the full Phase 0 check: `bash scripts/edge-tuning/edge_phase0.sh`

## Optional — XDP filter (`xdp/`)

Kernel-level DROP for blocklisted IPs, SYN/PPS floods on tracker port. Syncs maps from Redis via `edge-bpf-sync`.

```bash
# Build image (from repo root)
docker build -f deploy/edge/xdp/Dockerfile -t espx-edge-xdp .

# Run (privileged, host network)
docker run --rm -it --privileged --network host \
  -e INGRESS_INTERFACE=eth0 \
  -e BROKER_REDIS_URL=redis://127.0.0.1:6379/0 \
  espx-edge-xdp
```

BPF source: `xdp/bpf/edge_filter.c` (also used by `go generate` in `internal/edge/bpf`).

Manual BPF object build: `make -C deploy/edge/xdp`
