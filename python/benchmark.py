import argparse
import threading
import time
from typing import Union

import influxdb
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteOptions


class Writer:
    def __init__(self, influxdb_client: Union[InfluxDBClient, influxdb.InfluxDBClient], batch_size: int,
                 flush_interval: int):
        self.client = influxdb_client

        if isinstance(influxdb_client, InfluxDBClient):
            self.write_api = self.client.write_api(WriteOptions(batch_size=batch_size, flush_interval=flush_interval))

    def write(self, id: int, measurement_name: str, iteration: int):
        pass


class WriterV1(Writer):
    def write(self, id: int, measurement_name: str, iteration: int, batch_size=0, flush_interval=0):
        line = "%s,id=%s temperature=%d %d" % (measurement_name, id, time.time(), iteration)
        self.client.write_points(points=line, protocol="line")

    def countRows(self):
        pass


class WriterV2(Writer):
    def write(self, id: int, measurement_name: str, iteration: int):
        line = "%s,id=%s temperature=%d %d" % (measurement_name, id, time.time(), iteration)
        self.write_api.write(bucket="my-bucket", org="my-org", record=line)

    def countRows(self, measurement_name):
        query = '''from(bucket:"my-bucket")
        |> range(start: 0, stop: now())
        |> filter(fn: (r) => r._measurement == "{}")
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> drop(columns: ["id", "host"]) |> count(column: "temperature")'''.format(measurement_name)

        result = self.client.query_api().query(query=query)
        if (len(result) > 0):
            return result[0].records[0]['temperature']
        return 0


def worker(stop_event, id: int, measurement_name, seconds_count, line_protocol_count):
    start_time = time.time()
    for i in range(1, seconds_count + 1):
        iteration_time_start = time.time()
        if stop_event.is_set():
            return

        if id == 0:
            print("writing iterations:: {}/{} id:{}".format(i, seconds_count, id))

        start = i * line_protocol_count
        stop = start + line_protocol_count

        for j in range(start, stop):

            if time.time() > start_time + seconds_count:
                print(f"Time elapsed {id}", id)
                return

            if stop_event.is_set():
                return
            writer.write(id, measurement_name, j)

        if stop_event.is_set():
            return

        iteration_duration = time.time() - iteration_time_start
        print(f"iteration duration {iteration_duration:.2f}s")

        if iteration_duration < 1:
            time.sleep(1 - iteration_duration)


def main():
    global writer

    parser = argparse.ArgumentParser(description='InfluxDB 2.0 client lib benchmark')
    parser.add_argument("-type", default="CLIENT_PYTHON_V2",
                        help="Type of writer (default 'CLIENT_PYTHON_V2'; CLIENT_PYTHON_V1, CLIENT_PYTHON_V2)")
    # 2000
    parser.add_argument("-threadsCount", type=int, default=200, help="how much Thread use to write into InfluxDB")
    parser.add_argument("-secondsCount", type=int, default=60, help="how long write into InfluxDB")
    parser.add_argument("-batchSize", type=int, default=50000, help="batch size")
    parser.add_argument("-flushInterval", type=int, default=10000, help="buffer flush interval")
    parser.add_argument("-lineProtocolsCount", type=int, default=10, help="how much data writes in one batch")
    parser.add_argument("-skipCount", action="store_true", help="skip query for counting rows on end of benchmark")
    parser.add_argument("-measurementName", default="sensor_%d" % (time.time()), help="measurement name")

    args = parser.parse_args()

    writer_type = args.type
    threads_count = args.threadsCount
    seconds_count = args.secondsCount
    batch_size = args.batchSize
    flush_interval = args.flushInterval
    line_protocols_count = args.lineProtocolsCount
    skip_count = args.skipCount
    measurement_name = args.measurementName

    expected = threads_count * seconds_count * line_protocols_count

    print()
    print("------------- %s -------------" % writer_type)
    print()
    print()
    print("measurement:        %s " % measurement_name)
    print("threadsCount:       %s " % threads_count)
    print("secondsCount:       %s " % seconds_count)
    print("lineProtocolsCount: %s " % line_protocols_count)
    print()
    print("batchSize:          %s " % batch_size)
    print("flushInterval:      %s " % flush_interval)
    print()
    print("expected size: %s" % expected)
    print()
    print(args.type)

    if writer_type == 'CLIENT_PYTHON_V2':
        writer = WriterV2(
            influxdb_client=InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=False),
            batch_size=batch_size,
            flush_interval=flush_interval)
    if writer_type == 'CLIENT_PYTHON_V1':
        writer = WriterV1(influxdb.InfluxDBClient('localhost', 8086, 'root', 'root', 'iot_writes'))

    stop_event = threading.Event()

    if threads_count > 1:
        threads = []
        for i in range(threads_count):
            # print("Prepare thread %d " % i)
            t = threading.Thread(target=worker, daemon=True,
                                 args=(stop_event, i, measurement_name, seconds_count, line_protocols_count))
            threads.append(t)

        for thread in threads:
            thread.start()

        # sleep main thread
        time.sleep(seconds_count)

        # send stop event to all threads
        print("Stop event!")
        stop_event.set()

        # wait threads to finish
        for thread in threads:
            thread.join(timeout=seconds_count)

    else:
        worker(stop_event, 0, measurement_name, seconds_count, line_protocols_count)

    if not skip_count:
        count = writer.countRows(measurement_name=measurement_name)

        print()
        print("Results:")
        print(f"-> expected:        {expected} ")
        print(f"-> total:           {count} ")
        print(f"-> rate [%]:        {(count / expected) * 100} ")
        print(f"-> rate [msg/sec]:  {count / seconds_count} ")

        print("Written records {}:{}".format(writer_type, count))


if __name__ == '__main__':
    main()
