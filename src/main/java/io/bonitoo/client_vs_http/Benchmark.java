package io.bonitoo.client_vs_http;

import java.util.concurrent.TimeUnit;

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

import static org.fusesource.jansi.Ansi.ansi;

/**
 * Write data into InfluxDB through client.
 */
@SuppressWarnings({"UnstableApiUsage"})
public class Benchmark {

    private static final String INFLUX_DB_URL = "http://localhost:8086";
    private static final String INFLUX_DB_DATABASE = "iot_writes";

    public static void main(String[] args) throws ParseException {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        Options cmdOptions = new Options();

        cmdOptions.addOption(Option.builder("help").desc("Print this help").hasArg(false).build());
        cmdOptions.addOption(Option.builder("type").desc("Type of writer (default \"CLIENT_V1\"; CLIENT_V1, CLIENT_V1_OPTIMIZED, HTTP_V1)").hasArg().build());
        cmdOptions.addOption(Option.builder("threadsCount").desc("how much Thread use to write into InfluxDB").hasArg().build());
        cmdOptions.addOption(Option.builder("secondsCount").desc("how long write into InfluxDB").hasArg().build());
        cmdOptions.addOption(Option.builder("lineProtocolsCount").desc("how much data writes in one batch").hasArg().build());

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
        } else {
            throw new ParseException("The: " + type + " is not supported");
        }

        writer.start().verify();

        stopWatch.stop();
        System.out.println();
        System.out.println("Total time: " + stopWatch.toString());
        System.out.println("-----------------------------------------");
    }

    private static class Client_V1 extends AbstractIOTWriter {
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

    private static class HTTP_V1 extends AbstractIOTWriter {
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
}
