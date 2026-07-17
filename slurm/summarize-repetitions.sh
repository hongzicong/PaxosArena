#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

run_dir=$1
expected_repetitions=$2
summary="$run_dir/summary.csv"
details="$run_dir/repetition-summaries.csv"
regions=(
    ap-east-1 ap-northeast-1 ap-southeast-2 eu-west-1 ca-central-1
    sa-east-1 us-east-1 us-east-2 us-west-1 us-west-2 OVERALL
)

shopt -s nullglob
files=("$run_dir"/repetition-*/results/summary.csv)
if [[ ${#files[@]} != "$expected_repetitions" ]]; then
    echo "Expected $expected_repetitions repetition summaries but found ${#files[@]}" >&2
    exit 1
fi

printf 'Repetition,Region,Count,MeanMs,MedianMs,P95Ms,P99Ms,MinMs,MaxMs\n' > "$details"
for file in "${files[@]}"; do
    repetition=$(basename "$(dirname "$(dirname "$file")")")
    awk -F, -v repetition="$repetition" 'NR > 1 { print repetition "," $0 }' "$file" >> "$details"
done

printf 'Region,Repetitions,TotalSamples,MeanMsAvg,MeanMsStdDev,MedianMsAvg,MedianMsStdDev,P95MsAvg,P95MsStdDev,P99MsAvg,P99MsStdDev,MinMsAvg,MinMsStdDev,MaxMsAvg,MaxMsStdDev\n' > "$summary"
for region in "${regions[@]}"; do
    awk -F, -v region="$region" -v expected="$expected_repetitions" '
        $1 == region {
            repetitions++
            samples += $2
            for (column = 3; column <= 8; column++) {
                sum[column] += $column
                sumsq[column] += $column * $column
            }
        }
        END {
            if (repetitions != expected) {
                printf "Expected %d rows for %s but found %d\n", expected, region, repetitions > "/dev/stderr"
                exit 1
            }
            printf "%s,%d,%d", region, repetitions, samples
            for (column = 3; column <= 8; column++) {
                average = sum[column] / repetitions
                variance = repetitions > 1 ? (sumsq[column] - sum[column] * sum[column] / repetitions) / (repetitions - 1) : 0
                if (variance < 0 && variance > -1e-9) variance = 0
                printf ",%.3f,%.3f", average, sqrt(variance)
            }
            printf "\n"
        }
    ' "${files[@]}" >> "$summary"
done

cat "$summary"
printf 'Repetition details: %s\n' "$details"
printf 'Repetition summary: %s\n' "$summary"
