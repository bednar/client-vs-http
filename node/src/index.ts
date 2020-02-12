import { parseOptions, printOptions } from "./options";
import Writer from "./Writer";

async function run() {
  // parse nad print options
  const options = parseOptions();
  printOptions(options);

  // run writers
  const startTime = Date.now();
  const writer = new Writer(options);
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
  await writer.writeApi.close();

  // print results
  const elapsedTime = Date.now() - startTime;
  if (!options.skipCount) {
    const count = await writer.insertedCount();
    const expected = options.expectedSize;
    console.log(`==============================
Results:
-> expected:        ${expected}
-> total  :         ${writer.totalCount}
-> inserted:        ${count}
-> rate [%]:        ${(count / expected) * 100}
-> rate [msg/sec]:  ${count / (elapsedTime / 1000)}
`);
  }
}

run().catch(e => console.error(e));
