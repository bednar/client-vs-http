import {
  InfluxDB,
  WriteApi,
  WritePrecision,
  FluxTableMetaData
} from "@bonitoo-io/influxdb-client";
import { Options } from "./options";

export default class Writer {
  client: InfluxDB;
  writeApi: WriteApi;
  totalCount = 0;

  constructor(readonly options: Options) {
    this.client = new InfluxDB({
      url: options.url,
      token: options.token,
      writeOptions: {
        batchSize: options.batchSize,
        flushInterval: options.flushInterval
      }
    });
    this.writeApi = this.client.getWriteApi(
      options.org,
      options.bucket,
      WritePrecision.s
    );
  }

  async write(writerId: number): Promise<void> {
    const options = this.options;
    const writeApi = this.writeApi;
    const measurementName = this.options.measurementName;

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
          writeApi.writeRecord(
            `${measurementName},id=${writerId} temperature=${Date.now()} ${count++}`
          );
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
    const query =
      'from(bucket:"my-bucket")\n' +
      "  |> range(start: 0, stop: now())\n" +
      '  |> filter(fn: (r) => r._measurement == "' +
      this.options.measurementName +
      '")\n' +
      '  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")\n' +
      '  |> drop(columns: ["id", "host"])\n' +
      '  |> count(column: "temperature")';

    return new Promise((resolve, reject) => {
      let val = 0;
      this.client.getQueryApi(this.options.org).queryRows(query, {
        next(row: string[], tableMeta: FluxTableMetaData): void {
          if (!val) val = parseInt(row[tableMeta.column("temperature").index]);
        },
        error(error: Error) {
          reject(error);
        },
        complete() {
          resolve(val);
        }
      });
    });
  }
}
