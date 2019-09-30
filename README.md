# client-vs-http

## Build

Clone and run

```shell script
mvn clean compile assembly:single
```                                                            

```shell script
java -jar target/client-vs-http-jar-with-dependencies.jar -help    

usage: java -cp target/client-vs-http-jar-with-dependencies.jar [-help] [-lineProtocolsCount <arg>] 
       [-secondsCount <arg>] [-threadsCount <arg>] [-type <arg>]
 -help                       Print this help
 -lineProtocolsCount <arg>   how much data writes in one batch
 -secondsCount <arg>         how long write into InfluxDB
 -threadsCount <arg>         how much Thread use to write into InfluxDB
 -type <arg>                 Type of writer (default 'CLIENT_V1'; CLIENT_V1, CLIENT_V1_OPTIMIZED, HTTP_V1, CLIENT_V2, CLIENT_V2_OPTIMIZED, HTTP_V2)
```          

## Run

Show _error_ log from InfluxDB 1.x

```shell script
docker logs -f influxdb 2>&1| grep -v "204"
```  

Start benchmark

```shell script
./scripts/benchmark.sh
```     

## Results:

### Influx DB 1.7 OSS

|                                   |  rate [%]:    | rate [msg/sec]    |
|-----------------------------------|---------------|-------------------|
|  Client                           | -             | -                 |
|  Client - optimized batch options | -             | -                 |
|  Pure Java HTTP                   | -             | -                 |

### Influx DB 2 OSS

|                                   |  rate [%]:    | rate [msg/sec]    |
|-----------------------------------|---------------|-------------------|
|  Client                           | -             | -                 |
|  Client - optimized batch options | -             | -                 |
|  Pure Java HTTP                   | -             | -                 |