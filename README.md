# client-vs-http

## Build

Clone and run

```shell script
mvn clean compile assembly:single

java -jar target/client-vs-http-jar-with-dependencies.jar -help
```                                                            

```shell script
java -jar target/client-vs-http-jar-with-dependencies.jar -help    

usage: java -cp target/client-vs-http-jar-with-dependencies.jar [-help] [-lineProtocolsCount <arg>] 
       [-secondsCount <arg>] [-threadsCount <arg>] [-type <arg>]
 -help                       Print this help
 -lineProtocolsCount <arg>   how much data writes in one batch
 -secondsCount <arg>         how long write into InfluxDB
 -threadsCount <arg>         how much Thread use to write into InfluxDB
 -type <arg>                 Type of writer (default "CLIENT"; CLIENT, HTTP)
```
