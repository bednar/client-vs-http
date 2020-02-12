import { Options } from "./options";

export default abstract class Writer {
  totalCount = 0;

  constructor(readonly options: Options) {}

  abstract async write(writerId: number): Promise<void>;
  abstract async insertedCount(): Promise<number>;
  abstract async close(): Promise<void>;
}
