#!/usr/bin/env bash
# Step 3: spill on the budget-stall shape (noaa-eri-pds recording, replayed with its 98 ms p50 page latency).
source ${REPLAY_ENV:?path to the replay env.sh}
S=${S:?scratch dir with swath-s3}; G=${G:-$S/gate3}; B=${B:-noaa-eri-pds}
swath-replay serve --fixture ${BENCH_DATA:?set BENCH_DATA}/recordings/$B/dataset/data --bucket $B --host 127.0.0.1 --port 19320 \
  --serving-mode sorted --metrics-port 19420 --parquet-connections 8 --max-concurrent-requests 1024 \
  --inject-latency "worker_page=98ms,pivot_probe=98ms,structure_probe=98ms" > $G/replay.log 2>&1 &
SRV=$!; trap 'kill $SRV' EXIT
until curl -sf http://127.0.0.1:19420/healthz >/dev/null; do sleep 1; done
for rep in 1 2; do
  for arm in no-spill spill spill-zstd; do
    a=""; [ $arm = no-spill ] && a="-no-spill"; [ $arm = spill-zstd ] && a="-spill-compress"
    rm -rf $G/out
    /usr/bin/time -f '%e %U %S %M' -o $G/t $S/swath-s3 list s3://$B -endpoint http://127.0.0.1:19320 -path-style -anonymous \
      -workers 64 -seed -out $G/out $a -json > $G/$arm-$rep.json 2>/dev/null
    read -r wall user sys rss < $G/t
    python3 - $G/$arm-$rep.json "$arm" "$rep" "$wall" "$rss" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); s=d['s3']; sp=d.get('spill',{})
print(f"{sys.argv[2]}\t{sys.argv[3]}\twall={sys.argv[4]}\trss_mib={int(sys.argv[5])//1024}\trequests={s['requests']}\tmean_active={s['mean_active']:.1f}\tsink_wait_s={s['sink_wait_ns']/1e9:.0f}\tbusy_s={s['busy_ns']/1e9:.0f}\tmax_buffered={d['max_buffered_bytes']}\tfrontier_wait_s={d['frontier_wait_ns']/1e9:.0f}\tbudget_wait_s={d['budget_wait_ns']/1e9:.0f}\tspill_rows={sp.get('rows',0)}\tspill_bytes={sp.get('bytes',0)}\tspill_ranges={sp.get('ranges',0)}")
PY
    md5sum < <(cat $G/out/data/*.parquet) | cut -c1-12
  done
done
echo DONE
