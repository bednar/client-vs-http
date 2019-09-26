package io.bonitoo.client_vs_http;

import java.util.concurrent.TimeUnit;

import io.bonitoo.influxdb.reactive.InfluxDBReactive;
import io.bonitoo.influxdb.reactive.InfluxDBReactiveFactory;
import io.bonitoo.influxdb.reactive.options.BatchOptionsReactive;
import io.bonitoo.influxdb.reactive.options.InfluxDBOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Write data into InfluxDB through client.
 */
@SuppressWarnings({"UnstableApiUsage", "StatementWithEmptyBody"})
public class Benchmark {
    public static void main(String[] args) throws ParseException {

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
        if ("CLIENT".equals(type)) {
            client(line);
        } else {
            throw new ParseException("The: " + type + " is not supported");
        }
    }

    private static void client(final CommandLine line) {

        InfluxDBOptions options = InfluxDBOptions.builder()
                .url("http://localhost:8086")
                .database("iot_writes")
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
}
