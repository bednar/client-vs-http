#!/usr/bin/env bash
#
# The MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

set -e

threadsCount=10
secondsCount=10
lineProtocolsCount=100

SCRIPT_PATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"

cd "${SCRIPT_PATH}"/../
echo "Compile java benchmarks..."
mvn clean compile assembly:single &>/dev/null

function java_bench() {
  java -jar "${SCRIPT_PATH}"/../target/client-vs-http-jar-with-dependencies.jar -type "$1" \
    -threadsCount ${threadsCount} -secondsCount ${secondsCount} -lineProtocolsCount ${lineProtocolsCount}
}

function warmup() {
  echo "Warming DB "$i
  case "$i" in
  *V1*)
    echo "Warming V1"
    java_bench CLIENT_V1
    ;;
  *V2*)
    echo "Warming V2"
    java_bench CLIENT_V2
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
  echo "$(docker exec -it $container ps -f -p 1 -o time -h |  awk -F: '{ print ($1 * 3600) + ($2 * 60) + $3 }')"
}

timestamp() {
   date '+%s%N' --date="$1"
}

declare -a types=("CLIENT_V1_OPTIMIZED" "CLIENT_V1" "HTTP_V1" "CLIENT_V2_OPTIMIZED" "CLIENT_V2" "HTTP_V2")

for i in "${types[@]}"; do
  echo "Restarting docker images..."
  "${SCRIPT_PATH}"/influxdb-restart.sh &>/dev/null

  echo "Warmup iteration..."
  warmup $i &>/dev/null

  echo "Start benchmark "$i
  cpu_start="$(influxdb_stats $i)"
  java_bench $i

  cpu_stop="$(influxdb_stats $i)"
  cpu_final=$(($cpu_stop - $cpu_start))
  echo "Benchmark: " $i", used influxdb CPU time [s]: "$cpu_final

done
