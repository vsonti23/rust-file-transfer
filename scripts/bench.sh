#!/usr/bin/env bash
# Loopback throughput benchmark for rft.
#
# Usage: scripts/bench.sh [size_mib] [runs]
# Builds a release binary, creates a random file, and sends it over
# 127.0.0.1 with several stream/chunk-size combinations. Reports the median
# wall-clock time per configuration (hashing + transfer + server verify).
set -euo pipefail

SIZE_MIB=${1:-1024}
RUNS=${2:-3}
PORT=${PORT:-7979}
WORK=${WORK:-$(mktemp -d)}
BIN=target/release/rft

cargo build --release --quiet
mkdir -p "$WORK/src" "$WORK/dst"
SRC="$WORK/src/bench.bin"
if [[ ! -f $SRC ]] || [[ $(stat -c %s "$SRC") -ne $((SIZE_MIB * 1024 * 1024)) ]]; then
  head -c $((SIZE_MIB * 1024 * 1024)) /dev/urandom > "$SRC"
fi

"$BIN" serve --dir "$WORK/dst" --listen "127.0.0.1:$PORT" >/dev/null 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
sleep 0.5

echo "file: ${SIZE_MIB} MiB, runs per config: $RUNS (median shown)"
printf "%-8s %-8s %-8s %10s %12s\n" streams chunk window seconds MiB/s
for cfg in "1 256K 8" "1 1M 8" "4 1M 8" "8 1M 8" "4 4M 4" "1 1M 1"; do
  read -r streams chunk window <<<"$cfg"
  times=()
  for _ in $(seq "$RUNS"); do
    rm -f "$WORK/dst/bench.bin"
    sync
    start=$(date +%s.%N)
    "$BIN" send "$SRC" --to "127.0.0.1:$PORT" --streams "$streams" \
      --chunk-size "$chunk" --window "$window" --no-progress >/dev/null
    end=$(date +%s.%N)
    times+=("$(awk -v a="$start" -v b="$end" 'BEGIN {print b - a}')")
  done
  median=$(printf "%s\n" "${times[@]}" | sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}')
  rate=$(awk -v s="$SIZE_MIB" -v t="$median" 'BEGIN {print s / t}')
  printf "%-8s %-8s %-8s %10.2f %12.0f\n" "$streams" "$chunk" "$window" "$median" "$rate"
done
