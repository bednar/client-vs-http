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

Configuration:
* threadsCount = 2000
* secondsCount = 30
* lineProtocolsCount = 100

Hardware:
* MacBook Pro (15-inch, 2017)
* Processor: 2,8 GHz Intel Core i7
* Memory: 16 GB 2133 MHz LPDDR3
* macOS Mojave 10.14.6 (18G95)


### Influx DB 1.7 OSS

|                                   |  rate [%]:    | rate [msg/sec]    |
|-----------------------------------|---------------|-------------------|
|  Client                           | 46.1433333    | 92286.6666        |
|  Client - optimized batch options | 100.0	        | 200000.0          |
|  Pure Java HTTP                   | 1.691216666   | 3382.43333        |

### Influx DB 2 OSS

|                                   |  rate [%]:    | rate [msg/sec]    |
|-----------------------------------|---------------|-------------------|
|  Client                           | 36.716766     | 73433.53          |
|  Client - optimized batch options | 69.5602       | 139120.4          |
|  Pure Java HTTP                   | 1.266933      | 2533.86666666666	|