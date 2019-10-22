#!/usr/bin/env bash

set -e

threadsCount=10
secondsCount=10
lineProtocolsCount=100
measurementName=sensor_$RANDOM$RANDOM
expectedCount=$(($threadsCount * $secondsCount * $lineProtocolsCount))

SCRIPT_PATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"

cd "${SCRIPT_PATH}"/../
echo "Compile java benchmarks..."
mvn -quiet clean compile assembly:single
echo "Compile go benchmarks"
cd "${SCRIPT_PATH}"/../go
go build -o ./bin/benchmark ./cmd/main.go

function run_benchmark() {

  case "$1" in
  *GO*)
    "${SCRIPT_PATH}"/../go/bin/benchmark \
      -measurementName ${measurementName} -threadsCount ${threadsCount} -secondsCount ${secondsCount} -lineProtocolsCount ${lineProtocolsCount} -skipCount
    ;;
  *)
    java -jar "${SCRIPT_PATH}"/../target/client-vs-http-jar-with-dependencies.jar -type "$1" \
      -measurementName ${measurementName} -threadsCount ${threadsCount} -secondsCount ${secondsCount} -lineProtocolsCount ${lineProtocolsCount} -skipCount
    ;;
  esac
}

function warmup() {
  echo "Warming DB "$i
  case "$i" in
  *V1*)
    echo "Warming V1"
    run_benchmark CLIENT_V1
    ;;
  *V2*)
    echo "Warming V2"
    run_benchmark CLIENT_V2
    ;;
  esac
}

function influxdb_stats() {
  case "$i" in
  *V1*)
    container="influxdb"
    ;;
  *V2*)
    container="influxdb_v2"
    ;;
  esac
  echo "$(docker exec -it $container ps -f -p 1 -o time -h | awk -F: '{ print ($1 * 3600) + ($2 * 60) + $3 }')"
}

function count_rows() {
  case "$i" in
  *V1*)
    curl -sS 'http://localhost:8086/query?pretty=true' \
      --data-urlencode "db=iot_writes" \
      --data-urlencode "q=select count(*) from ${measurementName}" | jq -M '.results[].series[].values[][-1]'
    ;;

  *V2*)
    curl http://localhost:9999/api/v2/query?org=my-org -XPOST -sS \
      -H 'Authorization: Token my-token' \
      -H 'Accept: application/csv' \
      -H 'Content-type: application/vnd.flux' \
      -d 'from(bucket:"my-bucket")
      |> range(start: 0, stop: now())
      |> filter(fn: (r) => r._measurement == "'${measurementName}'")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> drop(columns: ["id"]) |> count(column: "temperature")' | grep $measurementName | cut -d, -f7 | sed 's/[^0-9]*//g'
    ;;
  esac
}

declare -a types=("CLIENT_GO_V2" "CLIENT_V1_OPTIMIZED" "CLIENT_V1" "HTTP_V1" "CLIENT_V2_OPTIMIZED" "CLIENT_V2" "HTTP_V2")

for i in "${types[@]}"; do
  echo "Restarting docker images..."
  "${SCRIPT_PATH}"/influxdb-restart.sh &>/dev/null


  echo "Warmup iteration..."
  measurementName=sensor_$RANDOM$RANDOM
  warmup $i &>/dev/null

  echo "Start benchmark "$i
  cpu_start="$(influxdb_stats $i)"
  measurementName=sensor_$RANDOM$RANDOM
  run_benchmark $i

  cpu_stop="$(influxdb_stats $i)"
  cpu_final=$(($cpu_stop - $cpu_start))
  echo "cputime "$i":" $cpu_final
  echo "Query records count..."
  count=$(count_rows)
  echo "Written records: "$count
  echo "Rate %: " $(bc <<<"scale=2; 100 * $count / $expectedCount")
  echo "Rate msg/sec: " $(bc <<<"scale=2; $count / $secondsCount")

done
