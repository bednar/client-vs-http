package io.bonitoo.client_vs_http;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxTable;
import io.bonitoo.influxdb.reactive.InfluxDBReactive;
import io.bonitoo.influxdb.reactive.InfluxDBReactiveFactory;
import io.bonitoo.influxdb.reactive.options.BatchOptionsReactive;
import io.bonitoo.influxdb.reactive.options.InfluxDBOptions;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.time.StopWatch;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import static org.fusesource.jansi.Ansi.ansi;

/**
 * Write data into InfluxDB through client.
 */
@SuppressWarnings({"UnstableApiUsage"})
public class Benchmark {

    private static final String INFLUX_DB_URL = "http://localhost:8086";
    private static final String INFLUX_DB_DATABASE = "iot_writes";
    private static final String INFLUX_DB_2_URL = "http://localhost:9999";
    private static final String INFLUX_DB_2_ORG = "my-org";
    private static final String INFLUX_DB_2_BUCKET = "my-bucket";
    private static final String INFLUX_DB_2_TOKEN = "my-token";
    private static final WritePrecision INFLUX_DB_2_PRECISION = WritePrecision.NS;

    public static void main(String[] args) throws ParseException {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        Options cmdOptions = new Options();

        cmdOptions.addOption(Option.builder("help").desc("Print this help").hasArg(false).build());
        cmdOptions.addOption(Option.builder("type").desc("Type of writer (default \"CLIENT_V1\"; CLIENT_V1, CLIENT_V1_OPTIMIZED, HTTP_V1, CLIENT_V2, CLIENT_V2_OPTIMIZED, HTTP_V2)").hasArg().build());
        cmdOptions.addOption(Option.builder("threadsCount").desc("how much Thread use to write into InfluxDB").hasArg().build());
        cmdOptions.addOption(Option.builder("secondsCount").desc("how long write into InfluxDB").hasArg().build());
        cmdOptions.addOption(Option.builder("lineProtocolsCount").desc("how much data writes in one batch").hasArg().build());
        cmdOptions.addOption(Option.builder("measurementName").desc("target measurement name").hasArg().build());
        cmdOptions.addOption(Option.builder("skipCount").desc("skip query count records on end of the benchmark").hasArg(false).build());

        CommandLineParser parser = new DefaultParser();
        // parse the command line arguments
        CommandLine line = parser.parse(cmdOptions, args);
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(2000);
            formatter.printHelp("java -cp target/client-vs-http-jar-with-dependencies.jar", cmdOptions, true);
            return;
        }

        String type = line.getOptionValue("type", "CLIENT_V1");
        System.out.println();
        System.out.println("------------- " + ansi().fgBlue().a((type)).reset() + " -------------");
        System.out.println();

        AbstractIOTWriter writer;
        if ("CLIENT_V1".equals(type)) {
            writer = new Client_V1(line);
        } else if ("CLIENT_V1_OPTIMIZED".equals(type)) {
            BatchOptionsReactive batchOptions = BatchOptionsReactive.builder()
                    .bufferLimit(100_000_000)
                    .batchSize(200_000)
                    .flushInterval(10)
                    .build();
            writer = new Client_V1(line, batchOptions);
        } else if ("HTTP_V1".equals(type)) {
            writer = new HTTP_V1(line);
        } else if ("CLIENT_V2".equals(type)) {
            writer = new Client_V2(line);
        } else if ("CLIENT_V2_OPTIMIZED".equals(type)) {
            WriteOptions writeOptions = WriteOptions.builder()
                    .bufferLimit(100_000_000)
                    .batchSize(200_000)
                    .flushInterval(10)
                    .build();
            writer = new Client_V2(line, writeOptions);
        } else if ("HTTP_V2".equals(type)) {
            writer = new HTTP_V2(line);
        } else {
            throw new ParseException("The: " + type + " is not supported");
        }

        writer.start().verify();

        stopWatch.stop();
        System.out.println();
        System.out.println("Total time: " + stopWatch.toString());
        System.out.println("-----------------------------------------");
    }

    private static class Client_V1 extends AbstractV1IOTWriter {
        private final InfluxDBReactive client;

        Client_V1(final CommandLine line) {
            this(line, BatchOptionsReactive.builder().bufferLimit(100_000_000).build());
        }

        Client_V1(final CommandLine line, final BatchOptionsReactive batchOptions) {
            super(line);

            InfluxDBOptions options = InfluxDBOptions.builder()
                    .url(INFLUX_DB_URL)
                    .database(INFLUX_DB_DATABASE)
                    .precision(TimeUnit.SECONDS)
                    .build();

            client = InfluxDBReactiveFactory.connect(options, batchOptions);
        }

        @Override
        void writeRecord(final String records) {
            client.writeRecord(records);
        }

        @Override
        void finished() {
            client.close();
        }
    }

    private static class HTTP_V1 extends AbstractV1IOTWriter {
        private final OkHttpClient client;

        HTTP_V1(final CommandLine line) {
            super(line);

            client = new OkHttpClient.Builder().build();
        }

        @Override
        void writeRecord(final String records) throws Exception {
            Request request = new Request.Builder()
                    .url(INFLUX_DB_URL + "/write?db=" + INFLUX_DB_DATABASE)
                    .addHeader("accept", "application/json")
                    .post(RequestBody.create(MediaType.parse("text/plain"), records))
                    .build();

            Response response = client.newCall(request).execute();
            if (response != null) {
                response.close();
            }
        }

        @Override
        void finished() {
            client.dispatcher().executorService().shutdown();
            client.connectionPool().evictAll();
        }
    }

    private static class Client_V2 extends AbstractV2IOTWriter {
        private final InfluxDBClient client;
        private final WriteApi writeApi;

        Client_V2(final CommandLine line) {
            this(line, WriteOptions.builder().bufferLimit(100_000_000).build());
        }

        Client_V2(final CommandLine line, final WriteOptions writeOptions) {
            super(line);

            client = InfluxDBClientFactory.create(INFLUX_DB_2_URL, INFLUX_DB_2_TOKEN.toCharArray());
            writeApi = client.getWriteApi(writeOptions);
            LogManager.getLogManager().reset();
        }

        @Override
        void writeRecord(final String records) {
            writeApi.writeRecord(INFLUX_DB_2_BUCKET, INFLUX_DB_2_ORG, INFLUX_DB_2_PRECISION, records);
        }

        @Override
        void finished() {
            try {
                client.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class HTTP_V2 extends AbstractV2IOTWriter {
        private final OkHttpClient client;

        HTTP_V2(final CommandLine line) {
            super(line);

            client = new OkHttpClient.Builder().build();
        }

        @Override
        void writeRecord(final String records) throws Exception {
            Request request = new Request.Builder()
                    .url(INFLUX_DB_2_URL + "/api/v2/write?org=" + INFLUX_DB_2_ORG + "&bucket=" + INFLUX_DB_2_BUCKET + "&precision=" + INFLUX_DB_2_PRECISION.getValue())
                    .addHeader("accept", "application/json")
                    .addHeader("Authorization", "Token " + INFLUX_DB_2_TOKEN)
                    .post(RequestBody.create(MediaType.parse("text/plain"), records))
                    .build();

            Response response = client.newCall(request).execute();
            if (response != null) {
                response.close();
            }
        }

        @Override
        void finished() {
            client.dispatcher().executorService().shutdown();
            client.connectionPool().evictAll();
        }
    }

    private static abstract class AbstractV1IOTWriter extends AbstractIOTWriter {
        AbstractV1IOTWriter(final CommandLine line) {
            super(line);
        }

        Double countInDB() {

            InfluxDB client = InfluxDBFactory.connect(INFLUX_DB_URL);
            QueryResult result = client
                    .setDatabase("iot_writes")
                    .query(new Query("select count(*) from " + measurementName, "iot_writes"));
            client.close();

            return ((Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1));
        }
    }

    private static abstract class AbstractV2IOTWriter extends AbstractIOTWriter {
        AbstractV2IOTWriter(final CommandLine line) {
            super(line);
        }

        Double countInDB() {

            System.out.println("Querying InfluxDB 2.0...");
            
            String query = "from(bucket:\"my-bucket\")\n"
                    + "  |> range(start: 0, stop: now())\n"
                    + "  |> filter(fn: (r) => r._measurement == \"" + measurementName + "\")\n"
                    + "  |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n"
                    + "  |> drop(columns: [\"id\"])\n"
                    + "  |> count(column: \"temperature\")";

            InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                    .url(INFLUX_DB_2_URL)
                    .authenticateToken(INFLUX_DB_2_TOKEN.toCharArray())
                    .okHttpClient(new OkHttpClient.Builder().readTimeout(30, TimeUnit.SECONDS))
                    .build();

            try (InfluxDBClient client = InfluxDBClientFactory.create(options)) {

                List<FluxTable> results = client.getQueryApi().query(query, INFLUX_DB_2_ORG);

                Long count = (Long) results.get(0).getRecords().get(0).getValueByKey("temperature");
                assert count != null;

                return count.doubleValue();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
