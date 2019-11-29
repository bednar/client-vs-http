import argparse
import multiprocessing
import threading
import concurrent.futures
import time
from multiprocessing import JoinableQueue
from queue import Queue

import influxdb
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteOptions


class Writer(multiprocessing.Process):
    def __init__(self, queue):
        multiprocessing.Process.__init__(self)
        self.queue = queue
        self.terminated = False

    def write_line_protocol(self, line_protocol: str):
        pass

    def run(self):
        while True:
            next_task = self.queue.get()
            if next_task is None:
                # Poison pill means terminate
                self.terminate()
                self.queue.task_done()
                break
            if self.terminated:
                break
            self.write_line_protocol(line_protocol=next_task)
            self.queue.task_done()

    def terminate(self) -> None:
        self.terminated = True
        super().terminate()


class WriterV1(Writer):
    def __init__(self, queue):
        Writer.__init__(self, queue)
        self.client = influxdb.InfluxDBClient('localhost', 8086, 'root', 'root', 'iot_writes')

    def write_line_protocol(self, line_protocol: str):
        self.client.write_points(points=line_protocol, protocol="line")

    def terminate(self) -> None:
        super().terminate()
        self.client.close()


class WriterV2(Writer):
    def __init__(self, queue):
        Writer.__init__(self, queue)
        self.client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=True)
        self.write_api = self.client.write_api(WriteOptions(batch_size=50_000, flush_interval=10_000))

    def write_line_protocol(self, line_protocol: str):
        self.write_api.write(bucket="my-bucket", org="my-org", record=line_protocol)

    def terminate(self) -> None:
        super().terminate()
        # self.write_api.__del__()
        self.client.__del__()


def worker(stop_event, generator_id: int, measurement_name, seconds_count, line_protocol_count, queue: Queue):
    for i in range(1, seconds_count + 1):

        if stop_event.is_set():
            return

        if generator_id == 1:
            print("{}writing iterations: {}/{} ".format('\b' * 30, i, seconds_count), end="")

        start = i * line_protocol_count
        stop = start + line_protocol_count

        for j in range(start, stop):
            if stop_event.is_set():
                return
            line_protocol = f"{measurement_name},id={generator_id} temperature=1 {j}"
            process_queue_.put(line_protocol)

        if stop_event.is_set():
            return

        time.sleep(1)


# class Generator(multiprocessing.Process):
#     def __init__(self, queue, generator_id, threads_count, measurement_name, seconds_count, line_protocol_count):
#         multiprocessing.Process.__init__(self)
#         self.globalQueue = queue
#         # self.processQueue = multiprocessing.Queue()
#         self.generator_id = generator_id
#         self.threads_count = threads_count
#         self.measurement_name = measurement_name
#         self.seconds_count = seconds_count
#         self.line_protocol_count = line_protocol_count
#         self.stop_event = threading.Event()
#         # global process_shared_queue  # Initialize module-level global.
#         # process_shared_queue = self.queue
#
#     def run(self):
#         # print(f"start generator {self.generator_id}")
#         threads = []
#
#         print(f"Threads count: {self.threads_count}")
#         start = self.generator_id * self.threads_count
#         stop = start + self.threads_count
#
#         for i in range(start, stop):
#             t = threading.Thread(target=worker, daemon=True,
#                                  args=(self.stop_event, i, self.measurement_name, self.seconds_count,
#                                        self.line_protocol_count,
#                                        self.globalQueue))
#             threads.append(t)
#
#         for thread in threads:
#             thread.start()
#
#         # while not self.stop_event.is_set():
#         #     try:
#         #         get = self.processQueue.get()
#         #         self.globalQueue.put(get)
#         #         # if self.generator_id == 1:
#         #         # print(f'data: {get}')
#         #     except:
#         #         pass
#
#         print("BREAKED!!!!")
#
#         time.sleep(self.seconds_count)
#         self.stop_event.set()
#
#         # wait to finish
#         for thread in threads:
#             thread.join()
#
#     def terminate(self) -> None:
#         super().terminate()
#         self.stop_event.set()


def generator_fn(generator_id):
    threads = []

    stop_event = threading.Event()

    start = generator_id * process_threads_per_process_
    stop = start + process_threads_per_process_

    for i in range(start, stop):
        t = threading.Thread(target=worker, daemon=True,
                             args=(
                                 stop_event, i, process_measurement_name_, process_seconds_count_,
                                 process_line_protocols_count_,
                                 process_queue_))
        threads.append(t)

    for thread in threads:
        thread.start()

    time.sleep(process_seconds_count_)
    stop_event.set()
    print("FInish")

    # wait to finish
    for thread in threads:
        thread.join()


def init_generator(q, tpp, measurement_name, seconds_count, line_protocols_count):
    global process_queue_
    process_queue_ = q
    global process_threads_per_process_
    process_threads_per_process_ = tpp
    global process_measurement_name_
    process_measurement_name_ = measurement_name
    global process_seconds_count_
    process_seconds_count_ = seconds_count
    global process_line_protocols_count_
    process_line_protocols_count_ = line_protocols_count


def main():
    # global queue_

    parser = argparse.ArgumentParser(description='InfluxDB 2.0 client lib benchmark')
    parser.add_argument("-type", default="CLIENT_PYTHON_V2",
                        help="Type of writer (default 'CLIENT_PYTHON_V2'; CLIENT_PYTHON_V1, CLIENT_PYTHON_V2)")
    # 2000
    delimiter = 20
    parser.add_argument("-threadsCount", type=int, default=int(2000 / delimiter), help="how much Thread use to write into InfluxDB")
    parser.add_argument("-secondsCount", type=int, default=30, help="how long write into InfluxDB")
    parser.add_argument("-lineProtocolsCount", type=int, default=100*delimiter, help="how much data writes in one batch")
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

    # manager = multiprocessing.Manager()
    queue_ = JoinableQueue()
    time.sleep(1)
    # stop_event = threading.Event()

    if writer_type == 'CLIENT_PYTHON_V2':
        writer = WriterV2(queue_)
    elif writer_type == 'CLIENT_PYTHON_V1':
        writer = WriterV1(queue_)
    else:
        raise Exception(f'Not supported writer type: {writer_type}')

    # writer.start()
    writer.daemon = False
    writer.start()
    # writer.join() #

    cpu_count = multiprocessing.cpu_count()
    threads_per_process = int(threads_count / cpu_count)
    threads_per_process = 1 if threads_per_process < 1 else threads_per_process

    with concurrent.futures.ProcessPoolExecutor(cpu_count, initializer=init_generator,
                                                initargs=(queue_, threads_per_process, measurement_name, seconds_count,
                                                          line_protocols_count)) as executor:

        executor.map(generator_fn, range(min(cpu_count, threads_count)))

    generators = []
    time.sleep(seconds_count)

    print("terminate")
    queue_.put(None)
    writer.terminate()

    for generator in generators:
        generator.terminate()

    print()
    print("Stop event!")
    # stop_event.set()

    # wait threads to finish
    # for thread in threads:
    #     thread.join()

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
        print("Results:")
        print(f"-> expected:        {expected} ")
        print(f"-> total:           {count} ")
        print(f"-> rate [%]:        {(count / expected) * 100} ")
        print(f"-> rate [msg/sec]:  {count / seconds_count} ")


if __name__ == '__main__':
    main()
