package io.bonitoo.client_vs_http;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import io.bonitoo.influxdb.reactive.InfluxDBReactiveFactory;
import io.bonitoo.influxdb.reactive.options.InfluxDBOptions;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.apache.commons.cli.CommandLine;
import org.influxdb.dto.Query;

/**
 * @author Jakub Bednar (26/09/2019 12:21)
 */
abstract class AbstractIOTWriter {

    private final String measurementName;
    private final int threadsCount;
    private final int secondsCount;
    private final int lineProtocolsCount;

    AbstractIOTWriter(CommandLine line) {

        measurementName = "sensor_" + System.currentTimeMillis();
        threadsCount = Integer.parseInt(line.getOptionValue("threadsCount", "2000"));
        secondsCount = Integer.parseInt(line.getOptionValue("secondsCount", "30"));
        lineProtocolsCount = Integer.parseInt(line.getOptionValue("lineProtocolsCount", "100"));

        System.out.println();
        System.out.println("measurement:        " + measurementName);
        System.out.println("threadsCount:       " + threadsCount);
        System.out.println("secondsCount:       " + secondsCount);
        System.out.println("lineProtocolsCount: " + lineProtocolsCount);
        System.out.println();
    }

    AbstractIOTWriter start() {

        System.out.println("expected size: " + threadsCount * secondsCount * lineProtocolsCount);
        System.out.println();

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        for (int i = 1; i < threadsCount + 1; i++) {
            Runnable worker = new IOTSensor(i, measurementName, secondsCount, lineProtocolsCount);
            executor.execute(worker);
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {

        }

        System.out.println();
        System.out.println();

        finished();

        return this;
    }

    void verify() {
        InfluxDBOptions options = InfluxDBOptions.builder()
                .url("http://localhost:8086")
                .database("iot_writes")
                .build();

        InfluxDBReactiveFactory.connect(options)
                .query(new Query("select count(*) from " + measurementName, "iot_writes"))
                .subscribe(queryResult -> {

                    Double count = (Double) queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(1);
                    
                    System.out.println("Total number of written data points: " + count);
                });
    }

    abstract void writeRecord(final String records);

    abstract void finished();

    private class IOTSensor implements Runnable {

        private final Integer id;
        private final String measurementName;
        private final int secondCount;
        private final int lineProtocolsCount;

        IOTSensor(Integer id, String measurementName, final int secondCount, final int lineProtocolsCount) {
            this.id = id;
            this.measurementName = measurementName;
            this.secondCount = secondCount;
            this.lineProtocolsCount = lineProtocolsCount;
        }

        @Override
        public void run() {

            Flowable
                    .intervalRange(0, secondCount, 0, 1, TimeUnit.SECONDS, Schedulers.trampoline())
                    .subscribe(i -> {

                        if (id == 1) {
                            if (i == 0) {

                                System.out.print("writing ");
                            }
                            System.out.print(String.format("... %s/%s ", i + 1, secondCount));
                        }

                        int start = (int) (i * lineProtocolsCount);
                        int end = start + lineProtocolsCount;
                        IntStream
                                .range(start, end)
                                .mapToObj(j -> String.format("%s,id=%s temperature=%d %d", measurementName, id, System.currentTimeMillis(), j))
                                .forEach(AbstractIOTWriter.this::writeRecord);
                    });
        }
    }
}
