
@echo off
:: curl & wget required
SET PATH=%PATH%;C:\cygwin64\bin\

docker kill influxdb
docker rm influxdb
docker kill influxdb_v2
docker rm influxdb_v2
docker network rm influx_network
docker network create -d bridge influx_network --subnet 192.168.0.0/24 --gateway 192.168.0.1

SET INFLUXDB_IMAGE=influxdb:1.7-alpine
echo.
echo Restarting InfluxDB [%INFLUXDB_IMAGE%] ...
echo.

docker pull --quiet %INFLUXDB_IMAGE% 
:: docker run --detach --name influxdb --network influx_network --publish 8086:8086 --publish 8089:8089/udp --volume %cd%\..\..\scripts\influxdb.conf:/etc/influxdb/influxdb.conf  %INFLUXDB_IMAGE%
docker run --detach --name influxdb --network influx_network --publish 8086:8086 %INFLUXDB_IMAGE%
echo.
echo Wait to start InfluxDB...
wget --quiet --spider --tries=20 --retry-connrefused --waitretry=5 http://localhost:8086/ping

docker exec -ti influxdb sh -c "influx -execute 'create database iot_writes'"

SET INFLUXDB_V2_IMAGE=quay.io/influxdb/influx:nightly
::
:: InfluxDB 2.0
::
echo.
echo Restarting InfluxDB 2.0 %INFLUXDB_V2_IMAGE% ... 
echo.

docker pull %INFLUXDB_V2_IMAGE%
docker run  --detach  --name influxdb_v2   --network influx_network   --publish 9999:9999  %INFLUXDB_V2_IMAGE%

echo.
echo Wait to start InfluxDB 2.0
wget --quiet --spider --tries=20 --retry-connrefused --waitretry=5 http://localhost:9999/metrics

docker exec -ti influxdb bash -c "apk add procps"
docker exec -ti influxdb_v2 bash -c "apt update -y && apt install -y procps"

echo.
echo Post onBoarding request, to setup initial user (my-user@my-password), org (my-org) and bucketSetup (my-bucket)
echo.
curl -s -X POST http://localhost:9999/api/v2/setup -H "accept: application/json"    -d "{ \"username\": \"my-user\", \"password\": \"my-password\", \"org\": \"my-org\", \"bucket\": \"my-bucket\",\"token\": \"my-token\" }"

