using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using InfluxDB.Client.Api.Domain;
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

        public AbstractIotWriter(List<CommandOption> options)
        {
            MeasurementName = Benchmark.GetOptionValue(GetOption(options, "measurementName"),
                            "sensor_" + CurrentTimeMillis());
            ThreadsCount = int.Parse(Benchmark.GetOptionValue(GetOption(options, "threadsCount"), "2000"));
            SecondsCount = int.Parse(Benchmark.GetOptionValue(GetOption(options, "secondsCount"), "30"));
            LineProtocolsCount = int.Parse(Benchmark.GetOptionValue(GetOption(options, "lineProtocolsCount"), "100"));
            SkipCount = GetOption(options, "skipCount").HasValue();
            ExpectedCount = ThreadsCount * SecondsCount * LineProtocolsCount;

            Console.WriteLine("measurement:        " + MeasurementName);
            Console.WriteLine("threadsCount:       " + ThreadsCount);
            Console.WriteLine("secondsCount:       " + SecondsCount);
            Console.WriteLine("lineProtocolsCount: " + LineProtocolsCount);
            Console.WriteLine();
        }
        
        private void CancelNotification()
        {
            Execute = false;
            Console.WriteLine("\n\nThe time: " + SecondsCount + " seconds elapsed! Stopping all writers");
        }

        public async Task<AbstractIotWriter> Start()
        {
            Console.WriteLine("expected size: " + ExpectedCount);
            Console.WriteLine();

            var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.CancelAfter(SecondsCount);
            cancellationTokenSource.Token.Register(CancelNotification);

            var block = new ActionBlock<int>(async _ => { await DoLoad(_); },
                            new ExecutionDataflowBlockOptions{CancellationToken = cancellationTokenSource.Token});
            
            try
            {
                for (var ii = 1; ii < ThreadsCount + 1; ii++)
                {
                    block.Post(ii);
                }

                block.Complete();
                await block.Completion;
            }
            catch (Exception n)
            {
                Console.WriteLine(n.Message);
            }

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

        public abstract void WriteRecord(string records);

        protected abstract void Finished();

        private CommandOption GetOption(List<CommandOption> options, string type)
        {
            return options.FindAll(o => type.Equals(o.ShortName))[0];
        }

        public static long CurrentTimeMillis()
        {
            var jan1St1970 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            return (long) (DateTime.UtcNow - jan1St1970).TotalMilliseconds;
        }

        private async Task DoLoad(int id)
        {
            for (var ii = 0; ii < SecondsCount && Execute; ii++)
            {
                if (!Execute)
                {
                    break;
                }

                //
                // Logging
                //
                if (id == 1)
                {
                    Console.WriteLine("\rwriting iterations: " + (ii + 1) + "/" + SecondsCount);
                }

                //
                // Generate data
                //
                var start = ii * LineProtocolsCount;
                var end = start + LineProtocolsCount;

                var records = Enumerable.Range(start, end).ToList().Select(r =>
                                                MeasurementName + "," + "id=" + id + " temperature=" +
                                                CurrentTimeMillis() + " " + r)
                                .ToList();

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
        }
    }
}