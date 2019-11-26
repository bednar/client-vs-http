import argparse
import threading
import time
from typing import Union

import influxdb
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteOptions


class Writer:
    def __init__(self, influxdb_client: Union[InfluxDBClient, influxdb.InfluxDBClient]):
        self.client = influxdb_client
        if isinstance(influxdb_client, InfluxDBClient):
            self.write_api = self.client.write_api(WriteOptions(batch_size=50_000, flush_interval=10_000))

    def write(self, id: int, measurement_name: str, iteration: int):
        pass


class WriterV1(Writer):
    def write(self, id: int, measurement_name: str, iteration: int):
        # thread = threading.current_thread()
        line = "%s,id=%s temperature=%d %d" % (measurement_name, id, time.time(), iteration)
        # print("\n %s - %s " % (thread.name, line))
        self.client.write_points(points=line, protocol="line")


class WriterV2(Writer):
    def write(self, id: int, measurement_name: str, iteration: int):
        # thread = threading.current_thread()
        line = "%s,id=%s temperature=%d %d" % (measurement_name, id, time.time(), iteration)
        # print("\n %s - %s " % (thread.name, line))
        self.write_api.write(bucket="my-bucket", org="my-org", record=line)


def worker(stop_event, id: int, measurement_name, seconds_count, line_protocol_count):
    # print("\nstart worker: " + threading.current_thread().name)
    for i in range(1, seconds_count + 1):

        if stop_event.is_set():
            return

        if id == 1:
            print("writing iterations:: {}/{} ".format(i, seconds_count), end="\r", flush=True)
            # print("\rwriting iterations: %d/%d \r" % (i, seconds_count))

        start = i * line_protocol_count
        stop = start + line_protocol_count

        for j in range(start, stop):
            if stop_event.is_set():
                return
            writer.write(id, measurement_name, j)

        if stop_event.is_set():
            return

        time.sleep(1)


def main():
    global writer

    parser = argparse.ArgumentParser(description='InfluxDB 2.0 client lib benchmark')
    parser.add_argument("-type", default="CLIENT_PYTHON_V2",
                        help="Type of writer (default 'CLIENT_PYTHON_V2'; CLIENT_PYTHON_V1, CLIENT_PYTHON_V2)")
    # 2000
    parser.add_argument("-threadsCount", type=int, default=2000, help="how much Thread use to write into InfluxDB")
    parser.add_argument("-secondsCount", type=int, default=60, help="how long write into InfluxDB")
    parser.add_argument("-lineProtocolsCount", type=int, default=10, help="how much data writes in one batch")
    parser.add_argument("-skipCount", action="store_true", help="skip query for counting rows on end of benchmark")
    parser.add_argument("-measurementName", default="sensor_%d" % (time.time()), help="measurement name")

    args = parser.parse_args()

    writer_type = args.type
    threads_count = args.threadsCount
    seconds_count = args.secondsCount
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
    print("expected size: %s" % expected)
    print()
    print(args.type)

    if writer_type == 'CLIENT_PYTHON_V2':
        writer = WriterV2(
            influxdb_client=InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=False))
    if writer_type == 'CLIENT_PYTHON_V1':
        writer = WriterV1(influxdb.InfluxDBClient('localhost', 8086, 'root', 'root', 'iot_writes'))

    threads = []
    stop_event = threading.Event()
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
        thread.join()


if __name__ == '__main__':
    main()
