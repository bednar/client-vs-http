import { parseOptions, printOptions, Options } from "./options";
import WriterV1 from "./WriterV1";
import WriterV2 from "./WriterV2";
import Writer from "./Writer";

export function createWriter(options: Options): Writer {
  if (options.type === "NODE_V2") {
    return new WriterV2(options);
  } else if (options.type === "NODE_V1") {
    return new WriterV1(options);
  } else {
    throw new Error(`Unsupported writer type: ${options.type}`);
  }
}

async function run() {
  // parse nad print options
  const options = parseOptions();
  printOptions(options);

  // run writers
  const startTime = Date.now();
  const writer = createWriter(options);
  const runners = new Array(options.threadsCount);
  console.log(
    `Estimated end time: ${new Date(Date.now() + options.secondsCount * 1000)}`
  );
  for (let workerId = 0; workerId < options.threadsCount; workerId++) {
    runners[workerId] = writer.write(workerId);
  }

  // wait for them to finish
  await new Promise(r => setTimeout(r, options.secondsCount * 1000));
  options.terminated = true;
  await Promise.all(runners);
  await writer.close();

  // print results
  const elapsedTime = Date.now() - startTime;
  if (!options.skipCount) {
    const count = await writer.insertedCount();
    const expected = options.expectedSize;
    console.log(`============= ${options.type} =============
Results:
-> expected:        ${expected}
-> inserted:        ${writer.totalCount}
-> persisted:       ${count}
-> rate [%]:        ${(count / expected) * 100}
-> rate [msg/sec]:  ${count / (elapsedTime / 1000)}
`);
  }
}

run().catch(e => console.error(e));
