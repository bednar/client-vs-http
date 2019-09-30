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

#threadsCount=20
#secondsCount=5
#lineProtocolsCount=10

SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"

cd "${SCRIPT_PATH}"/../
mvn clean compile assembly:single

declare -a types=("CLIENT_V1_OPTIMIZED" "CLIENT_V1" "HTTP_V1")
for i in "${types[@]}"
do
  "${SCRIPT_PATH}"/influxdb-restart.sh
   java -jar "${SCRIPT_PATH}"/../target/client-vs-http-jar-with-dependencies.jar -type "$i" \
      -threadsCount ${threadsCount} -secondsCount ${secondsCount} -lineProtocolsCount ${lineProtocolsCount}
   echo
   echo
   echo
done
