#!/usr/bin/env bash
# Two binaries, interleaved, on a bucket served by bench/s3fake (REPORT.md "Step 1: custom S3 client").
# usage: BEFORE=./swath-old AFTER=./swath ENDPOINT=http://127.0.0.1:9077 BUCKET=bench OUT=dir bench/client_gate.sh
set -euo pipefail
REPS=${REPS:-3}; OUT=${OUT:?}; BEFORE=${BEFORE:?}; AFTER=${AFTER:?}; ENDPOINT=${ENDPOINT:?}; BUCKET=${BUCKET:-bench}
mkdir -p "$OUT"; RAW=$OUT/raw.tsv
printf 'config\trep\tbinary\twall_s\tuser_s\tsys_s\tmaxrss_kb\tmd5\trequests\tretries\n' > "$RAW"
for rep in $(seq 1 "$REPS"); do
  for cfg in "sequential|-sequential" "w4|-workers 4" "w8|-workers 8" "w16|-workers 16"; do
    name=${cfg%%|*}; args=${cfg#*|}
    for which in before after; do
      bin=$BEFORE; [ "$which" = after ] && bin=$AFTER
      env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN AWS_EC2_METADATA_DISABLED=true \
        /usr/bin/time -f '%e %U %S %M' -o "$OUT/t" "$bin" list "s3://$BUCKET" -endpoint "$ENDPOINT" -path-style \
        -out "$OUT/ds-$which" -overwrite -json $args > "$OUT/json-$name-$which-$rep.json"
      read -r wall user sys rss < "$OUT/t"
      md5=$(cat "$OUT/ds-$which"/data/*.parquet | md5sum | cut -c1-12)
      req=$(python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print(d['s3']['requests'],d['s3']['retries'])" "$OUT/json-$name-$which-$rep.json")
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$rep" "$which" "$wall" "$user" "$sys" "$rss" "$md5" "${req// /	}" >> "$RAW"
      echo "$name #$rep $which wall=$wall md5=$md5"
    done
  done
done
