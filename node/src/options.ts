export interface Options {
  measurementName: number;
  threadsCount: number;
  secondsCount: number;
  lineProtocolsCount: number;

  url: string;
  org: string;
  bucket: string;
  token: string;

  batchSize: number;
  flushInterval: number;

  expectedSize: number;

  terminated?: boolean; // set by runtime to terminate all run ning processings
}

export function parseOptions(): Options {
  const program = require("commander");
  function intArg(arg: string) {
    console.log(arg);
    const retVal = parseInt(arg);
    if (!retVal || retVal < 0)
      throw new Error(`Invalid integer argument: ${arg}`);
    return retVal;
  }

  program.version(require("../package.json").version);
  program.description(require("../package.json").description);
  program.option(
    "--threadsCount <count>",
    "how much threads writes into InfluxDB",
    intArg,
    200
  );
  program.option("--secondsCount <count>", "time to write into InfluxDB", 60);
  program.option("--batchSize <size>", "write batch size", intArg, 50000);
  program.option(
    "--flushInterval <millis>",
    "buffer flush interval",
    intArg,
    10000
  );
  program.option(
    "--lineProtocolsCount <count>",
    "how much data writes in one batch",
    intArg,
    10
  );
  program.option(
    "--skipCount",
    "skip query for counting rows on end of benchmark"
  );
  program.option(
    "--measurementName <name>",
    "measurement name",
    "senzor" + Date.now()
  );
  program.option("--url <base>", "server base URL", "http://localhost:9999");
  program.option("--org <org>", "organization", "my-org");
  program.option("--token <token>", "authentication token", "my-token");
  program.option("--bucket <bucket>", "database bucket", "my-bucket");
  return program.parse(process.argv);
}
export function printOptions(program: Options): void {
  program.expectedSize =
    program.threadsCount * program.secondsCount * program.lineProtocolsCount;
  console.log(`------------------------------------
measurement:        ${program.measurementName}
threadsCount:       ${program.threadsCount}
secondsCount:       ${program.secondsCount}
lineProtocolsCount: ${program.lineProtocolsCount}

url:                ${program.url}
org:                ${program.org}
bucket:             ${program.bucket}

batchSize:          ${program.batchSize}
flushInterval:      ${program.flushInterval}

expectedSize:       ${program.expectedSize}
------------------------------------`);
}
