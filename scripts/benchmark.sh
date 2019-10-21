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

threadsCount=2000
secondsCount=30
lineProtocolsCount=100

#threadsCount=2
#secondsCount=5
#lineProtocolsCount=2

SCRIPT_PATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"

cd "${SCRIPT_PATH}"/../
mvn clean compile assembly:single
go build -o ./go/bin/benchmark ./go/cmd/main.go

declare -a types=("CLIENT_V1_OPTIMIZED" "CLIENT_V1" "HTTP_V1" "CLIENT_V2_OPTIMIZED" "CLIENT_V2" "HTTP_V2" "CLIENT_GO")
for i in "${types[@]}"; do
  "${SCRIPT_PATH}"/influxdb-restart.sh
  if [ "$i" != "CLIENT_GO" ]; then
    java -jar "${SCRIPT_PATH}"/../target/client-vs-http-jar-with-dependencies.jar -type "$i" \
      -threadsCount ${threadsCount} -secondsCount ${secondsCount} -lineProtocolsCount ${lineProtocolsCount}
  else
    "${SCRIPT_PATH}"/../go/bin/benchmark \
      -threadsCount ${threadsCount} -secondsCount ${secondsCount} -lineProtocolsCount ${lineProtocolsCount}
  fi
  echo
  echo
  echo
done
