#!/usr/bin/env bash
# Best-effort CPU governor for reproducible benchmarks on CI and self-hosted perf runners.
set -euo pipefail

if [[ -d /sys/devices/system/cpu/cpu0/cpufreq ]]; then
	echo "performance" | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor >/dev/null || true
fi
if command -v cpupower >/dev/null 2>&1; then
	sudo cpupower frequency-set -g performance >/dev/null || true
fi
