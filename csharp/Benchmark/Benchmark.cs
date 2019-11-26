using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core;
using InfluxDB.Client.Core.Flux.Domain;
using InfluxDB.Collector;
using InfluxDB.LineProtocol;
using InfluxDB.LineProtocol.Client;
using InfluxDB.LineProtocol.Payload;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.VisualBasic;

/**
 * @author Pavlina Rolincova (18/11/2019 10:21)
 */
namespace Benchmark
{
    class Benchmark
    {
        private static string InfluxDbUrl;
        private static string InfluxDb2Url;

        private const string InfluxDbDatabase = "iot_writes";
        private const string InfluxDb2Org = "my-org";
        private const string InfluxDb2Bucket = "my-bucket";
        private const string InfluxDb2Token = "my-token";
        private const WritePrecision InfluxDb2Precision = WritePrecision.Ns;

        public static void Main(string[] args)
        {
            var stopWatch = new Stopwatch();
            stopWatch.Start();

            var cmd = new CommandLineApplication();

            var helpOption = cmd.HelpOption("-? | -help");
            var argType = cmd.Option("-type <value>",
                            "Type of writer (default \"CLIENT_CSHARP_V1\"; CLIENT_CSHARP_V1, CLIENT_CSHARP_V2)",
                            CommandOptionType.SingleValue);
            var argThreadsCount = cmd.Option("-threadsCount <value>", "how much Thread use to write into InfluxDB",
                            CommandOptionType.SingleValue);
            var argSecondsCount = cmd.Option("-secondsCount <value>", "how long write into InfluxDB",
                            CommandOptionType.SingleValue);
            var argLineProtocolsCount = cmd.Option("-lineProtocolsCount <value>", "how much data writes in one batch",
                            CommandOptionType.SingleValue);
            var argMeasurementName = cmd.Option("-measurementName <value>", "target measurement name",
                            CommandOptionType.SingleValue);
            var argSkipCount = cmd.Option("-skipCount", "skip query count records on end of the benchmark",
                            CommandOptionType.NoValue);
            var argInfluxDb1 = cmd.Option("-influxDb1 <value>", "URL of InfluxDB v1; default: 'http://localhost:8086'",
                            CommandOptionType.SingleValue);
            var argInfluxDb2 = cmd.Option("-influxDb2 <value>", "URL of InfluxDB v2; default: 'http://localhost:9999'",
                            CommandOptionType.SingleValue);

            AbstractIotWriter writer = null;
            cmd.OnExecute(async () =>
            {
                var type = GetOptionValue(argType, "CLIENT_CSHARP_V2");

                Console.ForegroundColor = ConsoleColor.Blue;

                Console.WriteLine();
                Console.WriteLine("------------- " + type + " -------------");
                Console.WriteLine();

                Console.ResetColor();

                InfluxDbUrl = GetOptionValue(argInfluxDb1, "http://localhost:8086");
                InfluxDb2Url = GetOptionValue(argInfluxDb2, "http://localhost:9999");

                if (argInfluxDb1.HasValue())
                {
                    WriteDestination(InfluxDbUrl);
                }

                if (argInfluxDb2.HasValue())
                {
                    WriteDestination(InfluxDb2Url);
                }
                
                try
                {
                    if (type.Equals("CLIENT_CSHARP_V1"))
                    {
                        writer = new ClientV1(cmd.Options);
                    }
                    else if (type.Equals("CLIENT_CSHARP_V2"))
                    {

                        writer = new ClientV2(cmd.Options);
                    }
                    else if (type.Equals("CLIENT_CSHARP_EMPTY"))
                    {

                        writer = new NullWritter(cmd.Options);
                    }

                    else
                    {
                        throw new CommandParsingException(cmd, "The: " + type + " is not supported");
                    }
                    
                    await writer.Start();
                    writer.Verify();
                }
                catch (CommandParsingException e)
                {
                    Console.WriteLine(e);
                }

                return 0;
            });

            cmd.Execute(args);

            stopWatch.Stop();
            

            Console.WriteLine();
            Console.WriteLine("Total time: " + stopWatch.Elapsed);
            Console.WriteLine("-------------------------------------");
        }

        private static void WriteDestination(string destination)
        {
            Console.ForegroundColor = ConsoleColor.Red;

            Console.Write("destination: ");

            Console.ResetColor();

            Console.WriteLine(destination);

            Console.WriteLine();
        }

        public static string GetOptionValue(CommandOption option, string defaultValue)
        {
            return option.HasValue() ? option.Value() : defaultValue;
        }

        class NullWritter : AbstractIotWriterEx
        {
            public NullWritter(List<CommandOption> options) : base(options, InfluxDbDatabase, InfluxDbUrl, null)
            {
            }

            public NullWritter(List<CommandOption> options, string databaseName, string url, string token) : base(options, databaseName, url, token)
            {
            }

            protected override Task<double> CountInDb()
            {
                return Task.FromResult(Convert.ToDouble(Counter));
            }

            protected override void WriteRecord(string records)
            {
                Interlocked.Increment(ref Counter);
            }

            protected override void Finished()
            {
                return;
            }
        }
        class ClientV1 : AbstractIotWriterEx
        {
            private MetricsCollector _collector;
            
            public ClientV1(List<CommandOption> options) : base(options, InfluxDbDatabase, InfluxDbUrl, null)
            {
                _collector = new CollectorConfiguration()
                                .Batch.AtInterval(TimeSpan.FromSeconds(2))
                                .WriteTo.InfluxDB(InfluxDbUrl, InfluxDbDatabase)
                                .CreateCollector();
            }

            protected override void WriteRecord(string records)
            {
                var fields = records.Split(",")[1];
                var values = fields.Split();
                
                _collector.Write(MeasurementName, new Dictionary<string, object>
                {
                                {values[1].Split("=")[0], values[1].Split("=")[1]}

                }, new Dictionary<string, string> {
                                {values[0].Split("=")[0], values[0].Split("=")[1]}
                });
                Interlocked.Increment(ref Counter);
            }

            protected override void Finished()
            {
                _collector.Dispose();
            }
        }

        private class ClientV2 : AbstractIotWriterEx
        {
            private readonly InfluxDBClient _client;
            private readonly WriteApi _writeApi;

            public ClientV2(List<CommandOption> options) : this(options, WriteOptions.CreateNew().BatchSize(100000).FlushInterval(2000).Build())
            {

            }

            public ClientV2(List<CommandOption> options, WriteOptions writeOptions) : base(options, InfluxDb2Bucket, InfluxDb2Url, InfluxDb2Token)
            {
                InfluxDBClientOptions opts = InfluxDBClientOptions.Builder.CreateNew()
                    .Url(InfluxDb2Url)
                    .AuthenticateToken(InfluxDb2Token.ToCharArray())
                    .LogLevel(LogLevel.Headers).Build();
                _client = InfluxDBClientFactory.Create(opts);
                _writeApi = _client.GetWriteApi(writeOptions);
            }

            protected override void WriteRecord(string records)
            {
                _writeApi.WriteRecord(InfluxDb2Bucket, InfluxDb2Org, InfluxDb2Precision, records);
                Interlocked.Increment(ref Counter);
            }

            protected override void Finished()
            {
                try
                {
                    _writeApi.Flush();
                    _client.Dispose();
                }
                catch (Exception e)
                {
                    throw new SystemException(e.Message);
                }
            }
        }

        abstract class AbstractIotWriterEx : AbstractIotWriter
        {
            protected string url;
            protected string databaseName;
            protected string token; 
            
            
            protected AbstractIotWriterEx(List<CommandOption> options, string databaseName, string url, string token) : base(options)
            {
                this.databaseName = databaseName;
                this.url = url;
                this.token = token;
            }

            protected override async Task<double> CountInDb()
            {
                Console.WriteLine("Querying InfluxDB ...");

                var query = "from(bucket:\"" + databaseName + "\")\n"
                               + "  |> range(start: 0, stop: now())\n"
                               + "  |> filter(fn: (r) => r._measurement == \"" + MeasurementName + "\")\n"
                               + "  |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n"
                               + "  |> drop(columns: [\"id\", \"host\"])\n"
                               + "  |> count(column: \"temperature\")";

                var options = InfluxDBClientOptions.Builder.CreateNew()
                                .Url(url)
                                .AuthenticateToken(InfluxDb2Token.ToCharArray())
                                .Build();

                var client = InfluxDBClientFactory.Create(options);

                try
                {
                    var results = await client.GetQueryApi().QueryAsync(query, InfluxDb2Org);

                    var count = (long) results[0].Records[0].GetValueByKey("temperature");

                    return count;
                }
                catch (Exception e)
                {
                    throw new SystemException(e.Message);
                }
            }
        }
    }
}