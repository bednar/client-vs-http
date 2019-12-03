using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;

namespace Benchmark
{
    public abstract class AbstractIotWriter
    {
        protected readonly string MeasurementName;
        protected readonly int ThreadsCount;
        protected readonly int SecondsCount;
        protected readonly int LineProtocolsCount;
        protected readonly int ExpectedCount;
        protected static bool Execute = true;
        protected readonly bool SkipCount;
        public static int Counter = 0;

        public AbstractIotWriter(List<CommandOption> options)
        {
            MeasurementName = Benchmark.GetOptionValue(GetOption(options, "measurementName"),
                "sensor_" + CurrentTimeMillis());
            ThreadsCount = int.Parse(Benchmark.GetOptionValue(GetOption(options, "threadsCount"), "2000"));
            SecondsCount = int.Parse(Benchmark.GetOptionValue(GetOption(options, "secondsCount"), "60"));
            LineProtocolsCount = int.Parse(Benchmark.GetOptionValue(GetOption(options, "lineProtocolsCount"), "100"));
            SkipCount = GetOption(options, "skipCount").HasValue();
            ExpectedCount = ThreadsCount * SecondsCount * LineProtocolsCount;

            Console.WriteLine("measurement:        " + MeasurementName);
            Console.WriteLine("threadsCount:       " + ThreadsCount);
            Console.WriteLine("secondsCount:       " + SecondsCount);
            Console.WriteLine("lineProtocolsCount: " + LineProtocolsCount);
            Console.WriteLine();
        }

        public async Task<AbstractIotWriter> Start()
        {
            Console.WriteLine("expected size: " + ExpectedCount);
            Console.WriteLine();

            var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.CancelAfter(SecondsCount);

            var threads = new Collection<Thread>();
            for (int i = 0; i < ThreadsCount; i++)
            {
                Thread t = new Thread(new ParameterizedThreadStart(DoLoad));
                threads.Add(t);
                t.Start(i);
            }

            for (int i = 0; i < threads.Count; i++)
            {
                var thread = threads[i];
                thread.Join(SecondsCount * 1000);
            }

            Console.WriteLine("Writer counter: {0}", Counter);
            
            Console.WriteLine();
            Console.WriteLine();

            Finished();

            return this;
        }

        public void Verify()
        {
            if (SkipCount)
            {
                return;
            }

            var count = CountInDb().GetAwaiter().GetResult();

            Console.WriteLine("Results:");
            Console.WriteLine("-> expected:        " + ExpectedCount);
            Console.WriteLine("-> total:           " + count);
            Console.WriteLine("-> rate [%]:        " + (count / ExpectedCount) * 100);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("-> rate [msg/sec]:  " + count / SecondsCount);
            Console.ResetColor();
        }

        protected abstract Task<double> CountInDb();

        protected abstract void WriteRecord(string records);

        protected abstract void Finished();

        public CommandOption GetOption(List<CommandOption> options, string type)
        {
            return options.FindAll(o => type.Equals(o.ShortName))[0];
        }

        public static long CurrentTimeMillis()
        {
            var jan1St1970 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            return (long) (DateTime.UtcNow - jan1St1970).TotalMilliseconds;
        }

        private void DoLoad(object param)
        {
            int id = Convert.ToInt32(param);
            Console.Write("S:{0}",id);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var random = new Random();
            for (var ii = 0; ii < SecondsCount && Execute; ii++)
            {
                if (stopwatch.ElapsedMilliseconds >= SecondsCount * 1000)
                {
//                    Console.WriteLine("Time elapsed for thread: {0}, id={1}", Thread.CurrentThread.ManagedThreadId, id);
                    break;
                }

                if (!Execute)
                {
                    break;
                }

                //
                // Logging
                //
                if (id == 1)
                {
                    Console.WriteLine("writing iterations: " + (ii + 1) + "/" + SecondsCount);
                }
                
//                Console.Write(".");

                //
                // Generate data
                //
                var start = ii * LineProtocolsCount;
                var end = start + LineProtocolsCount;

                var records = new List<string>();
                for (int j = start; j < end; j++)
                {
                    var record = MeasurementName + "," + "id=" + id + " temperature="
                                 + random.Next(0, Int32.MaxValue) + " " + j;
                    records.Add(record);
                }

                //
                // Write records one by one
                //
                foreach (var record in records)
                {
                    if (Execute)
                    {
                        WriteRecord(record);
                    }
                }

                if (!Execute)
                {
                    break;
                }

                Thread.Sleep(1000);
            }
            Console.Write("E:{0} ", id);
        }
    }
}