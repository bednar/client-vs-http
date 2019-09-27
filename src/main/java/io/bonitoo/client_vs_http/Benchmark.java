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

/**
 * Write data into InfluxDB through client.
 */
@SuppressWarnings({"UnstableApiUsage", "StatementWithEmptyBody"})
public class Benchmark {

    private static final String INFLUX_DB_URL = "http://localhost:8086";
    private static final String INFLUX_DB_DATABASE = "iot_writes";

    public static void main(String[] args) throws ParseException {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        Options cmdOptions = new Options();

        cmdOptions.addOption(Option.builder("help").desc("Print this help").hasArg(false).build());
        cmdOptions.addOption(Option.builder("type").desc("Type of writer (default \"CLIENT\"; CLIENT, HTTP)").hasArg().build());
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

        String type = line.getOptionValue("type", "CLIENT");
        System.out.println();
        System.out.println("------------- " + type + " -------------");
        System.out.println();

        if ("CLIENT".equals(type)) {
            client(line);
        } else if ("HTTP".equals(type)) {
            http(line);
        } else {
            throw new ParseException("The: " + type + " is not supported");
        }

        stopWatch.stop();
        System.out.println();
        System.out.println("Total time: " + stopWatch.toString());
    }

    private static void client(final CommandLine line) {

        InfluxDBOptions options = InfluxDBOptions.builder()
                .url(INFLUX_DB_URL)
                .database(INFLUX_DB_DATABASE)
                .precision(TimeUnit.SECONDS)
                .build();

        BatchOptionsReactive batchOptions = BatchOptionsReactive.builder()
                .bufferLimit(6_000_000)
                .build();

        InfluxDBReactive client = InfluxDBReactiveFactory.connect(options, batchOptions);

        AbstractIOTWriter writer = new AbstractIOTWriter(line) {
            @Override
            void writeRecord(final String records) {
                client.writeRecord(records);
            }

            @Override
            void finished() {
                client.close();
                while (!client.isClosed()) {
                }
            }
        };

        writer.start().verify();
    }

    private static void http(final CommandLine line) {

        OkHttpClient client = new OkHttpClient.Builder().build();

        AbstractIOTWriter writer = new AbstractIOTWriter(line) {
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
            }
        };

        writer.start().verify();
    }
}
