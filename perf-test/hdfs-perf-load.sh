#!/usr/bin/env bash
# =====================================================================================
# AutoMQ-on-HDFS CONCURRENT load test. Run INSIDE a broker pod:
#   kubectl cp hdfs-perf-load.sh magnetar-test/automq-broker-rongyu-0:/tmp/load.sh
#   kubectl exec -it -n magnetar-test automq-broker-rongyu-0 -- bash /tmp/load.sh
#
# WHY this exists (vs hdfs-perf-test.sh):
#   hdfs-perf-test.sh runs ONE producer process for ~1.5-4s per case. That is a single
#   sender thread (idempotence caps in-flight to 5) moving 20-128 MB -- it never reaches
#   steady state and cannot saturate a 3-broker cluster. This script fixes all three:
#     1. MANY producer processes in parallel (PRODUCERS), each its own JVM/sender ->
#        real client concurrency, the thing the raw-WebHDFS sweep proved is the lever.
#     2. Each case moves enough data to run >= ~60s (TARGET_SECS x expected MB/s) so the
#        WAL upload pipeline reaches steady state (large WAL batches actually fill).
#     3. Consumer reports fetch.MB.sec (not the rebalance-polluted end-to-end number).
#   Total throughput = sum of all producer processes. That is the cluster's real ceiling.
#
# Tunables (env):
#   PRODUCERS=6         parallel producer processes (try 1,3,6,12 to find the knee)
#   PARTITIONS=12       topic partitions (>= PRODUCERS, ideally a multiple of broker count)
#   TARGET_SECS=60      per-case target duration; records-per-producer is sized from it
#   RECORD_SIZES="1024 16384 65536"   record sizes to sweep. Accepts raw BYTES
#                                     (>=1024, e.g. 16384) or KB shorthand
#                                     (<1024, e.g. "16" == 16384, "1 16 64").
#   BATCH=1048576       producer batch.size (1 MiB; high-RTT backend needs big batches)
#   LINGER=100          producer linger.ms
#   TOPIC_PREFIX=hdfs-load
#   BOOT=<all 3 pods>   bootstrap (defaults to the 3 broker pods via headless DNS)
#   NS=magnetar-test    namespace (for the default BOOT)
#   SVC=automq-broker-rongyu   headless service name (for the default BOOT)
#   RUN_CONSUMER=1      also run a consumer fetch-throughput pass (set 0 to skip)
# =====================================================================================
set -uo pipefail

BIN=/opt/automq/kafka/bin

# CRITICAL: when this script runs INSIDE a broker pod, the pod exports
# KAFKA_HEAP_OPTS=-Xms4g -Xmx4g -XX:MaxDirectMemorySize=8G for the broker JVM.
# kafka-run-class.sh only falls back to its 256M default when KAFKA_HEAP_OPTS is
# EMPTY -- so every kafka-producer-perf-test.sh we spawn would otherwise inherit a
# 4G heap. With PRODUCERS=N that is N x 4G of client heap sharing the broker's
# cgroup, which blows the memory limit and gets the broker OOMKilled (only the pod
# running this script dies -- exactly the symptom). Force a small client heap so the
# producers cost ~512M each and never starve the broker.
export KAFKA_HEAP_OPTS="${CLIENT_HEAP_OPTS:--Xms256m -Xmx512m}"
NS="${NS:-magnetar-test}"
SVC="${SVC:-automq-broker-rongyu}"
# Brokers via the headless Service, resolved with clusterset.local (NOT cluster.local).
# WHY: Falcon does not let you pin a workload to a specific standard cluster, so this
# load-gen pod may land on a DIFFERENT standard cluster than the brokers. clusterset.local
# is Falcon's cross-cluster discovery: for a headless Service it returns ALL broker pod
# IPs across every cluster in the fleet, so producers still find every partition leader.
# cluster.local only resolves same-cluster and fails when the pods are split across clusters.
BOOT="${BOOT:-${SVC}.${NS}.svc.clusterset.local:9092}"

PRODUCERS="${PRODUCERS:-6}"
PARTITIONS="${PARTITIONS:-12}"
TARGET_SECS="${TARGET_SECS:-60}"
RECORD_SIZES="${RECORD_SIZES:-1024 16384 65536}"
BATCH="${BATCH:-1048576}"
LINGER="${LINGER:-100}"
TOPIC_PREFIX="${TOPIC_PREFIX:-hdfs-load}"
RUN_CONSUMER="${RUN_CONSUMER:-1}"
WORK="$(mktemp -d)"

cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

# Rough per-PRODUCER throughput guess (MB/s) used ONLY to size num-records so each case
# runs ~TARGET_SECS. Being off just makes a case run a bit longer/shorter; not critical.
guess_mbps_per_producer() {  # $1 = record size bytes
  case "$1" in
    1024)  echo 6  ;;   # small records: per-producer ~6 MB/s
    16384) echo 12 ;;
    65536) echo 15 ;;
    *)     echo 10 ;;
  esac
}

echo "======================================================================"
echo "  AutoMQ-on-HDFS CONCURRENT load test"
echo "  bootstrap : $BOOT"
echo "  producers : $PRODUCERS parallel processes"
echo "  partitions: $PARTITIONS   target/case: ~${TARGET_SECS}s"
echo "  batch.size: $BATCH   linger.ms: $LINGER   acks=all"
echo "======================================================================"

run_case() {  # $1 = record size bytes
  local size="$1"
  local topic="${TOPIC_PREFIX}-${size}"
  local mbps; mbps="$(guess_mbps_per_producer "$size")"
  # num-records PER PRODUCER so each producer runs ~TARGET_SECS at the guessed rate.
  local num_per=$(( mbps * 1024 * 1024 * TARGET_SECS / size ))
  [ "$num_per" -lt 1000 ] && num_per=1000

  echo
  echo "############################################################"
  echo "# record ${size}B  x  ${PRODUCERS} producers  x  ${num_per} recs each"
  echo "#   (~$(( num_per * PRODUCERS * size / 1024 / 1024 )) MB total for this case)"
  echo "############################################################"

  "$BIN/kafka-topics.sh" --create --if-not-exists --topic "$topic" \
    --partitions "$PARTITIONS" --replication-factor 1 --bootstrap-server "$BOOT" \
    --config max.message.bytes=8388608 >/dev/null 2>&1

  : > "$WORK/results-$size"
  local start end wall
  start=$(date +%s%N)

  # Launch PRODUCERS processes in parallel. Each writes its own last-line summary to a
  # per-process file; we sum the MB/s across them for the cluster total.
  seq 1 "$PRODUCERS" | xargs -P "$PRODUCERS" -I{} bash -c '
    size="$1"; topic="$2"; num="$3"; boot="$4"; batch="$5"; linger="$6"; out="$7"; idx="{}"
    "'"$BIN"'/kafka-producer-perf-test.sh" --topic "$topic" \
      --num-records "$num" --record-size "$size" --throughput -1 \
      --producer-props bootstrap.servers="$boot" acks=all \
        batch.size="$batch" linger.ms="$linger" \
      2>/dev/null | tail -n 1 > "$out/p-$size-$idx.txt"
  ' _ "$size" "$topic" "$num_per" "$BOOT" "$BATCH" "$LINGER" "$WORK"

  end=$(date +%s%N)
  wall=$(awk -v s="$start" -v e="$end" 'BEGIN{printf "%.2f", (e-s)/1e9}')

  # Aggregate: pull "(X MB/sec)" and "N records sent" from each producer's summary line.
  local total_mbps total_recs wall_mbps
  total_mbps=$(cat "$WORK"/p-"$size"-*.txt 2>/dev/null \
    | grep -oE '\(([0-9.]+) MB/sec\)' | grep -oE '[0-9.]+' \
    | awk '{s+=$1} END{printf "%.2f", s}')
  total_recs=$(cat "$WORK"/p-"$size"-*.txt 2>/dev/null \
    | grep -oE '^[0-9]+ records sent' | grep -oE '^[0-9]+' \
    | awk '{s+=$1} END{print s}')
  # Wall-clock aggregate = total bytes / total elapsed time. This is the HONEST
  # cluster rate: it divides by ONE shared time window (first producer start ->
  # last producer end), so unlike the sum-of-per-producer number it does not
  # over-count by assuming every producer's peak overlapped. Always <= the sum.
  wall_mbps=$(awk -v r="${total_recs:-0}" -v sz="$size" -v w="$wall" \
    'BEGIN{ if (w>0) printf "%.2f", r*sz/1048576/w; else print "0" }')

  echo "--- per-producer summaries ---"
  cat "$WORK"/p-"$size"-*.txt 2>/dev/null | sed 's/^/  /'
  echo "--- AGGREGATE (record ${size}B) ---"
  echo "  producers      : $PRODUCERS"
  echo "  wall clock     : ${wall}s"
  echo "  total records  : ${total_recs:-0}"
  echo "  SUM-OF-PRODUCERS: ${total_mbps:-0} MB/s   (optimistic upper bound; peaks may not overlap)"
  echo "  WALL-CLOCK AGG  : ${wall_mbps:-0} MB/s   (honest: total bytes / shared time window)"
  # Persist the produced count so the consumer pass targets what actually exists
  # (hardcoding a larger --messages would make the consumer wait until --timeout).
  echo "${total_recs:-0}" > "$WORK/total-$size"
}

# Accept record sizes as EITHER raw bytes (>=1024, e.g. 16384) OR KB shorthand
# (<1024, e.g. 16 -> 16384). These tests never use sub-1KB records, so any value
# below 1024 is unambiguously KB. Downstream everything uses bytes.
to_bytes() {
  local v="$1"
  if [ "$v" -lt 1024 ]; then echo $(( v * 1024 )); else echo "$v"; fi
}

for sz in $RECORD_SIZES; do
  run_case "$(to_bytes "$sz")"
done

if [ "$RUN_CONSUMER" = "1" ]; then
  # Consume the largest-record topic; report ONLY fetch.MB.sec (rebalance-free number).
  csize="$(to_bytes "${RECORD_SIZES##* }")"   # last size in the list, normalized to bytes
  ctopic="${TOPIC_PREFIX}-${csize}"
  # Target the count that was ACTUALLY produced, else the consumer waits until --timeout.
  cmsgs=$(cat "$WORK/total-$csize" 2>/dev/null || echo 0)
  [ "${cmsgs:-0}" -lt 1 ] && cmsgs=1000
  echo
  echo "############################################################"
  echo "# CONSUMER fetch throughput  (topic ${ctopic}, ${cmsgs} msgs)"
  echo "############################################################"
  # warm-up group id so the metrics line reflects fetch, not first-connect.
  "$BIN/kafka-consumer-perf-test.sh" --topic "$ctopic" --bootstrap-server "$BOOT" \
    --messages "$cmsgs" --timeout 180000 --group "load-cons-$(date +%s)" 2>&1 \
    | awk -F',' 'NR==1{for(i=1;i<=NF;i++) gsub(/^ +| +$/,"",$i); print "  header:", $0}
                 NR==2{for(i=1;i<=NF;i++) gsub(/^ +| +$/,"",$i);
                       print "  data  :", $0;
                       print "  >>> fetch.MB.sec (col 9) =", $9, " fetch.nMsg.sec (col10) =", $10}'
fi

echo
echo "=== DONE ==="
echo "TOTAL THROUGHPUT per case = sum of all producer processes = the cluster ceiling."
echo "Sweep PRODUCERS=1,3,6,12 to find where throughput stops scaling."
