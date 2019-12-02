import argparse
import concurrent.futures
import multiprocessing
import time
from itertools import product
from time import sleep
from typing import Union, List

import influxdb
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteOptions


class Writer(multiprocessing.Process):
    def __init__(self, measurement_name: str, threads_count: int, seconds_count: int, line_protocols_count: int):
        multiprocessing.Process.__init__(self)
        self.terminated = False
        self.measurement_name = measurement_name
        self.threads_count = threads_count
        self.seconds_count = seconds_count
        self.line_protocols_count = line_protocols_count

    def write(self, records: List['str']):
        pass

    def run(self):
        data = generator(self.threads_count, self.line_protocols_count, self.seconds_count, self.measurement_name)

        for batch in data:
            if not self.terminated:
                for records in batch:
                    if not self.terminated:
                        self.write(records)

    def terminate(self) -> None:
        self.terminated = True
        super().terminate()


class WriterV1(Writer):
    def __init__(self, measurement_name: str, threads_count: int, seconds_count: int, line_protocols_count: int):
        Writer.__init__(self, measurement_name, threads_count, seconds_count, line_protocols_count)
        self.client = influxdb.InfluxDBClient('localhost', 8086, 'root', 'root', 'iot_writes')

    def write(self, records: List['str']):
        self.client.write_points(points=records, protocol="line", batch_size=50_000)

    def terminate(self) -> None:
        super().terminate()
        self.client.close()


class WriterV2(Writer):
    def __init__(self, measurement_name: str, threads_count: int, seconds_count: int, line_protocols_count: int):
        Writer.__init__(self, measurement_name, threads_count, seconds_count, line_protocols_count)
        self.client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=False)
        self.write_api = self.client.write_api(WriteOptions(batch_size=50_000, flush_interval=10_000))

    def write(self, records: List['str']):
        self.write_api.write(bucket="my-bucket", org="my-org", record=records)

    def terminate(self) -> None:
        super().terminate()
        self.client.__del__()


def init_generate_iteration(measure_name, number_of_line_protocols):
    global _measure_name
    _measure_name = measure_name
    global _number_of_line_protocols
    _number_of_line_protocols = number_of_line_protocols


def generate_iteration(generator_id, iteration):
    start = iteration * _number_of_line_protocols
    stop = start + _number_of_line_protocols
    return list(map(lambda x: f"{_measure_name},id={generator_id} temperature=1 {x}", range(start, stop)))


def generate_iteration_unpack(args):
    return generate_iteration(*args)


def generator(num_threads, number_of_line_protocols, number_of_seconds, measure_name):
    num_threads = num_threads

    def nested_generator():
        yield from _generator()

    def _generator():
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads, initializer=init_generate_iteration,
                                                   initargs=(measure_name, number_of_line_protocols)) as executor:
            for i in range(number_of_seconds):
                print("{}writing iterations: {}/{} ".format('\b' * 30, i + 1, number_of_seconds), end="")
                yield executor.map(generate_iteration_unpack, product(range(num_threads), [i]))
                sleep(1)

    return nested_generator()


def main():
    parser = argparse.ArgumentParser(description='InfluxDB 2.0 client lib benchmark')
    parser.add_argument("-type", default="CLIENT_PYTHON_V1",
                        help="Type of writer (default 'CLIENT_PYTHON_V2'; CLIENT_PYTHON_V1, CLIENT_PYTHON_V2)")
    # 2000
    parser.add_argument("-threadsCount", type=int, default=2000,
                        help="how much Thread use to write into InfluxDB")
    parser.add_argument("-secondsCount", type=int, default=60, help="how long write into InfluxDB")
    parser.add_argument("-lineProtocolsCount", type=int, default=100,
                        help="how much data writes in one batch")
    parser.add_argument("-skipCount", action="store_true", default=False,
                        help="skip query for counting rows on end of benchmark")
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

    if writer_type == 'CLIENT_PYTHON_V2':
        writer = WriterV2(measurement_name, threads_count, seconds_count, line_protocols_count)
    elif writer_type == 'CLIENT_PYTHON_V1':
        writer = WriterV1(measurement_name, threads_count, seconds_count, line_protocols_count)
    else:
        raise Exception(f'Not supported writer type: {writer_type}')

    writer.daemon = False
    writer.start()
    time.sleep(seconds_count)

    print()
    print("Terminating writer")
    print()
    writer.terminate()
    writer.kill()

    if not skip_count:
        count = 0
        if writer_type == 'CLIENT_PYTHON_V2':
            client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=False)
            query = f'from(bucket: "my-bucket") ' \
                    f'|> range(start: 0, stop: now()) ' \
                    f'|> filter(fn: (r) => r._measurement == "{measurement_name}") ' \
                    f'|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") ' \
                    f'|> drop(columns: ["id", "host"]) |> count(column: "temperature")'
            result = client.query_api().query(query=query, org="my-org")
            count = result[0].records[0]["temperature"]

        elif writer_type == 'CLIENT_PYTHON_V1':
            pass

        print()
        print()
        print("Results:")
        print(f"-> expected:        {expected} ")
        print(f"-> total:           {count} ")
        print(f"-> rate [%]:        {(count / expected) * 100} ")
        print(f"-> rate [msg/sec]:  {count / seconds_count} ")


if __name__ == '__main__':
    main()
