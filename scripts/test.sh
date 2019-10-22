#!/bin/bash


measurementName="sensor_1571656436756"

c=`curl http://localhost:9999/api/v2/query?org=my-org -XPOST -sS \
  -H 'Authorization: Token my-token' \
  -H 'Accept: application/csv' \
  -H 'Content-type: application/vnd.flux' \
  -d 'from(bucket:"my-bucket")
  |> range(start: 0, stop: now())
  |> filter(fn: (r) => r._measurement == "'${measurementName}'")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> drop(columns: ["id"]) |> count(column: "temperature")' | grep $measurementName | awk -F, '{print $7}' |sed 's/[^0-9]*//g' `


echo "---"
echo "A"${c}"a"
echo "A"${c}"ab"
echo "A"${c}"abc"
echo "A"${c}"abcd"
echo "---"

echo $((${c##*( )}*100))


curl http://localhost:9999/api/v2/query?org=my-org -XPOST -sS \
  -H 'Authorization: Token my-token' \
  -H 'Accept: application/csv' \
  -H 'Content-type: application/vnd.flux' \
  -d 'from(bucket:"my-bucket")
  |> range(start: 0, stop: now())
  |> filter(fn: (r) => r._measurement == "'${measurementName}'")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> drop(columns: ["id"]) |> count(column: "temperature")' | grep $measurementName | awk -F, '{print $7}' > tmp.txt




HEXVAL=$(xxd -pu <<< "${c}")
echo ${c} "ahoj"
echo $HEXVAL "ahoj"

#measurementName="sensor_1571660961349"

#curl -sS 'http://localhost:8086/query?pretty=true' --data-urlencode "db=iot_writes" --data-urlencode "q=select count(*) from ${measurementName}" | jq -M '.results[].series[].values[][-1]'


