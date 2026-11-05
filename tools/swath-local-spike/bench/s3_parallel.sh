#!/usr/bin/env bash
# S3 parallel core vs the sequential provider on a bucket served by
# bench/s3fake (REPORT.md "S3 parallel core 2026-11-05").
# usage: BIN=swath-local ENDPOINT=http://127.0.0.1:9077 BUCKET=bench OUT=dir bench/s3_parallel.sh
set -euo pipefail
REPS=${REPS:-3}; OUT=${OUT:?}; BIN=${BIN:?}; ENDPOINT=${ENDPOINT:?}; BUCKET=${BUCKET:-bench}
mkdir -p "$OUT"
RAW=$OUT/raw.tsv
printf 'config\trep\twall_s\tuser_s\tsys_s\tmaxrss_kb\tmd5\n' > "$RAW"
export AWS_EC2_METADATA_DISABLED=true
configs() {
  echo "sequential|-sequential"
  for w in 1 4 8 16; do
    echo "parallel w$w|-workers $w"
    echo "parallel+seed w$w|-workers $w -seed"
  done
}
for rep in $(seq 1 "$REPS"); do
  configs | while IFS='|' read -r name args; do
    /usr/bin/time -f '%e %U %S %M' -o "$OUT/t" "$BIN" list "s3://$BUCKET" -endpoint "$ENDPOINT" -path-style \
      -out "$OUT/ds" -overwrite -json $args > "$OUT/json-${name// /_}-$rep.json"
    read -r wall user sys rss < "$OUT/t"
    md5=$(cat "$OUT"/ds/data/*.parquet | md5sum | cut -c1-12)
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$rep" "$wall" "$user" "$sys" "$rss" "$md5" >> "$RAW"
    echo "$name #$rep wall=$wall md5=$md5"
  done
done
