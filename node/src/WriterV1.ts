import { InfluxDB, IPoint } from "influx";
import { Options } from "./options";
import Writer from "./Writer";

export default class WriterV1 extends Writer {
  client: InfluxDB;
  database = "iot_writes";
  batch: Array<IPoint>;
  batchSize = 0;
  intervalHandle: any;

  constructor(options: Options) {
    super(options);
    this.client = new InfluxDB({
      host: "localhost",
      database: this.database
    });
    this.batch = new Array(Math.max(options.batchSize, 1));
    this.intervalHandle = setInterval(
      this.flush.bind(this),
      this.options.flushInterval
    );
  }

  async write(writerId: number): Promise<void> {
    const options = this.options;
    const measurement = this.options.measurementName;

    let count = 0;
    for (let i = 0; i < options.secondsCount; i++) {
      if (options.terminated) {
        return;
      }
      if (writerId === 0) {
        console.log(`writing iteration: ${i + 1}/${options.secondsCount}`);
      }
      for (let j = 0; j < this.options.lineProtocolsCount; j++) {
        if (options.terminated) {
          return;
        } else {
          this.totalCount++;
          this.batch[this.batchSize++] = {
            measurement,
            tags: { id: String(writerId) },
            fields: { temperature: Date.now() },
            timestamp: count++
          };
          if (this.batchSize >= this.options.batchSize) {
            this.flush();
          }
        }
      }
      if (options.terminated) {
        return;
      }
      await new Promise(r => setTimeout(r, 1000));
    }
  }

  async insertedCount(): Promise<number> {
    // wait a few millis to get accurate data
    await new Promise(r => setTimeout(r, 500));
    const query = "select count(*) from " + this.options.measurementName;

    return new Promise((resolve, reject) => {
      this.client
        .queryRaw(query, { database: this.database })
        .then(response => {
          resolve(response.results[0].series[0].values[0][1]);
        })
        .catch(e => {
          reject(e);
        });
    });
  }
  async close(): Promise<void> {
    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
      this.intervalHandle = undefined;
    }
    return this.flush();
  }

  async flush(): Promise<void> {
    if (this.batchSize > 0) {
      this.batchSize = 0;
      const batch = this.batch;
      this.batch = new Array(Math.max(this.options.batchSize, 1));
      return this.client
        .writePoints(batch, {
          precision: "s",
          database: this.database
        })
        .catch(e => {
          console.debug(`Unable to write points`);
        });
    } else {
      return Promise.resolve();
    }
  }
}
