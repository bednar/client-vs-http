using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core.Flux.Domain;
using Microsoft.Extensions.CommandLineUtils;

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

            cmd.OnExecute(async () =>
            {
                var type = GetOptionValue(argType, "CLIENT_CSHARP_V1");

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
                    AbstractIotWriter writer;

                    if (type.Equals("CLIENT_CSHARP_V1"))
                    {
                        writer = new ClientV1(cmd.Options);
                    }
                    else if (type.Equals("CLIENT_CSHARP_V2"))
                    {

                        writer = new ClientV2(cmd.Options);
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

        class ClientV1 : AbstractV1IotWriter
        {
            public ClientV1(List<CommandOption> options) : base(options)
            {
            }

            public override void WriteRecord(string records)
            {
                
            }

            protected override void Finished()
            {
                
            }
        }

        private class ClientV2 : AbstractV2IotWriter
        {
            private readonly InfluxDBClient _client;
            private readonly WriteApi _writeApi;

            public ClientV2(List<CommandOption> options) : this(options, WriteOptions.CreateNew().Build())
            {

            }

            public ClientV2(List<CommandOption> options, WriteOptions writeOptions) : base(options)
            {
                _client = InfluxDBClientFactory.Create(InfluxDb2Url, InfluxDb2Token.ToCharArray());
                _writeApi = _client.GetWriteApi(writeOptions);
            }

            public override void WriteRecord(string records)
            {
                _writeApi.WriteRecord(InfluxDb2Bucket, InfluxDb2Org, InfluxDb2Precision, records);
            }

            protected override void Finished()
            {
                try
                {
                    _client.Dispose();
                }
                catch (Exception e)
                {
                    throw new SystemException(e.Message);
                }
            }
        }

        abstract class AbstractV1IotWriter : AbstractIotWriter
        {
            protected AbstractV1IotWriter(List<CommandOption> options) : base(options)
            {

            }

            protected override async Task<double> CountInDb()
            {
                /*InfluxDB client = InfluxDBFactory.connect(INFLUX_DB_URL);
                QueryResult result = client
                                .setDatabase("iot_writes")
                                .query(new Query("select count(*) from " + measurementName, "iot_writes"));
                client.close();

                return ((Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1));*/
                return 1;
            }
        }

        abstract class AbstractV2IotWriter : AbstractIotWriter
        {
            protected AbstractV2IotWriter(List<CommandOption> options) : base(options)
            {

            }

            protected override async Task<double> CountInDb()
            {
                Console.WriteLine("Querying InfluxDB 2.0...");

                var query = "from(bucket:\"my-bucket\")\n"
                               + "  |> range(start: 0, stop: now())\n"
                               + "  |> filter(fn: (r) => r._measurement == \"" + MeasurementName + "\")\n"
                               + "  |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n"
                               + "  |> drop(columns: [\"id\", \"host\"])\n"
                               + "  |> count(column: \"temperature\")";

                var options = InfluxDBClientOptions.Builder.CreateNew()
                                .Url(InfluxDb2Url)
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