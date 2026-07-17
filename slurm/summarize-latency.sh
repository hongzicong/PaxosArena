#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

run_dir=$1
results_dir="$run_dir/results"
config="$run_dir/config/cluster.conf"
summary="$results_dir/summary.csv"
raw_dir="$run_dir/raw-latency"
mkdir -p "$raw_dir"

regions=(
    ap-east-1 ap-northeast-1 ap-southeast-2 eu-west-1 ca-central-1
    sa-east-1 us-east-1 us-east-2 us-west-1 us-west-2
)
clones=$(awk '$1 == "clones:" { print $2; exit }' "$config")
expected_files=$((clones + 1))

percentile() {
    local sorted_file=$1
    local count=$2
    local percent=$3
    local index
    index=$(awk -v n="$count" -v p="$percent" 'BEGIN { i = int(n * p); if (i < n * p) i++; if (i < 1) i = 1; print i }')
    sed -n "${index}p" "$sorted_file"
}

write_row() {
    local region=$1
    local raw_file=$2
    local sorted_file="$raw_file.sorted"
    local count mean minimum maximum median p95 p99
    sort -n "$raw_file" > "$sorted_file"
    count=$(wc -l < "$sorted_file" | tr -d ' ')
    if [[ "$count" == 0 ]]; then
        echo "No latency samples found for $region" >&2
        exit 1
    fi
    read -r mean minimum maximum < <(
        awk 'NR == 1 { min = max = $1 } { sum += $1; if ($1 < min) min = $1; if ($1 > max) max = $1 } END { printf "%.3f %.3f %.3f\n", sum / NR, min, max }' "$sorted_file"
    )
    median=$(percentile "$sorted_file" "$count" 0.50)
    p95=$(percentile "$sorted_file" "$count" 0.95)
    p99=$(percentile "$sorted_file" "$count" 0.99)
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$region" "$count" "$mean" "$median" "$p95" "$p99" "$minimum" "$maximum" >> "$summary"
}

printf 'Region,Count,MeanMs,MedianMs,P95Ms,P99Ms,MinMs,MaxMs\n' > "$summary"
: > "$raw_dir/overall"
shopt -s nullglob
for region in "${regions[@]}"; do
    files=("$results_dir/${region}-client-"*)
    if [[ ${#files[@]} != "$expected_files" ]]; then
        echo "Expected $expected_files client log files for $region but found ${#files[@]}" >&2
        exit 1
    fi
    awk '/latency / { print $NF }' "${files[@]}" > "$raw_dir/$region"
    cat "$raw_dir/$region" >> "$raw_dir/overall"
    write_row "$region" "$raw_dir/$region"
done
write_row OVERALL "$raw_dir/overall"

cat "$summary"
printf 'Latency summary: %s\n' "$summary"
