import * as dotenv from "dotenv";
dotenv.config();

import * as fs from "fs";
import { createHash } from "crypto";
import * as lockfile from "lockfile";

import { Customer } from "./declarations";

import { MongoClient, Collection, ObjectId, FindCursor } from "mongodb";
import _ from "lodash";

const LOCKFILE = ".lock";
const POINTER_STORE = ".pointer";
const IS_FULL_REINDEX = "--full-reindex";

const DB_URI = process.env.DB_URI;
const SOURCE_COLLECTION = process.env.SOURCE_COLLECTION;
const TARGET_COLLECTION = process.env.TARGET_COLLECTION;
const BUFFER_SIZE = parseInt(process.env.BUFFER_SIZE || "");

function hex(value: string): string {
  const hash = createHash("sha256");
  hash.update(value);
  return hash.digest("hex").slice(0, 8);
}

function anonymizeCustomer(customer: Customer): Customer {
  customer.firstName = hex(customer.firstName);
  customer.lastName = hex(customer.lastName);
  const [loginPart, hostPart] = customer.email.split("@");
  customer.email = `${hex(loginPart)}@${hostPart}`;
  customer.address.line1 = hex(customer.address.line1);
  customer.address.line2 = hex(customer.address.line2);
  customer.address.postcode = hex(customer.address.postcode);
  return customer;
}

async function fullReindex(source: Collection, target: Collection) {
  let lastCreatedAt = null;

  console.log("[fullReindex]", "Update");
  let cursor = target.find<Customer>({}).sort({ createdAt: 1 });
  for await (const targetCustomer of cursor) {
    const sourceCustomer = await source.findOne<Customer>({
      _id: targetCustomer._id,
    });
    if (!sourceCustomer) continue;
    lastCreatedAt = targetCustomer.createdAt;
    const newTargetCustomer = anonymizeCustomer(sourceCustomer);
    if (_.isEqual(targetCustomer, newTargetCustomer)) continue;
    await target.updateOne(
      { _id: targetCustomer._id },
      { $set: newTargetCustomer }
    );
  }
  await cursor.close();

  if (!lastCreatedAt) {
    const firstCustomer = await source.findOne<Customer>(
      {},
      {
        sort: [["createdAt", 1]],
      }
    );
    if (!firstCustomer?.createdAt) return;
    lastCreatedAt = firstCustomer.createdAt;
    const aMinuteEarlier = lastCreatedAt.getTime() - 60 * 1000;
    lastCreatedAt = new Date(aMinuteEarlier);
  }

  console.log("[fullReindex]", "Insert newest", lastCreatedAt);
  cursor = source
    .find<Customer>({ createdAt: { $gt: lastCreatedAt } })
    .sort({ createdAt: 1 });
  for await (const sourceCustomer of cursor) {
    const newTargetCustomer = anonymizeCustomer(sourceCustomer);
    await target.insertOne(newTargetCustomer);
    lastCreatedAt = newTargetCustomer.createdAt;
  }
  await cursor.close();

  console.log("[fullReindex]", "Store new pointer", lastCreatedAt);
  const rawPointer = lastCreatedAt.toISOString();
  fs.writeFileSync(POINTER_STORE, rawPointer);
}

class WithLock {
  get locked(): boolean {
    return lockfile.checkSync(LOCKFILE);
  }
}

class Updater extends WithLock {
  source: Collection;
  target: Collection;

  cursor?: FindCursor;

  constructor(source: Collection, target: Collection) {
    super();

    this.source = source;
    this.target = target;
  }

  async buildStream() {
    if (this.cursor) await this.cursor.close();

    this.cursor = this.target.find({}).sort({ createdAt: 1 });
    const stream = this.cursor.stream();

    stream.on("data", async (targetCustomer: Customer) => {
      if (!this.locked) {
        await this.onData(targetCustomer);
      } else {
        this.cursor?.close();
        await this.run();
      }
    });

    stream.on("end", async () => {
      await this.run();
    });

    stream.on("error", (err) => {
      console.error(err);
    });
  }

  async onData(targetCustomer: Customer) {
    const sourceCustomer = await this.source.findOne<Customer>({
      _id: targetCustomer._id,
    });
    if (!sourceCustomer) return;
    const newTargetCustomer = anonymizeCustomer(sourceCustomer);
    if (_.isEqual(targetCustomer, newTargetCustomer)) return;
    await this.target.updateOne(
      { _id: targetCustomer._id },
      { $set: newTargetCustomer }
    );
  }

  async run() {
    if (this.locked) {
      console.log("[Updater]", "Updater has been locked");
      setTimeout(() => {
        this.run();
      }, 3000);
    } else {
      await this.buildStream();
    }
  }
}

class Pursuer extends WithLock {
  source: Collection;
  target: Collection;

  pointer?: Date;
  window: number = !_.isNaN(BUFFER_SIZE) ? BUFFER_SIZE : 1000;
  buffer: Customer[] = [];

  constructor(source: Collection, target: Collection) {
    super();

    this.source = source;
    this.target = target;
  }

  async loadPointer() {
    if (fs.existsSync(POINTER_STORE)) {
      const data = fs.readFileSync(POINTER_STORE);
      const rawPointer = data.toString();
      this.pointer = new Date(rawPointer);
    } else {
      const lastCustomer = await this.source.findOne<Customer>(
        {},
        {
          sort: [["createdAt", 1]],
        }
      );
      this.pointer = lastCustomer?.createdAt || new Date();
      const aMinuteEarlier = this.pointer.getTime() - 60 * 1000;
      this.pointer = new Date(aMinuteEarlier);
    }
  }

  async storePointer() {
    if (!this.pointer) return;
    const rawPointer = this.pointer?.toISOString();
    fs.writeFileSync(POINTER_STORE, rawPointer);
  }

  async loop() {
    if (!this.pointer) await this.loadPointer();

    const bufferAlreadyExists = this.buffer.length != 0;

    const cursor = this.source
      .find<Customer>({ createdAt: { $gt: this.pointer } })
      .sort({ createdAt: 1 })
      .limit(this.window - this.buffer.length)
      .skip(this.buffer.length);

    for await (const customer of cursor) {
      const newTargetCustomer = anonymizeCustomer(customer);
      this.buffer.push(newTargetCustomer);
    }

    if (this.buffer.length == this.window || bufferAlreadyExists) {
      await this.target.insertMany(this.buffer);
      const lastCustomer = this.buffer.at(-1);
      this.pointer = lastCustomer?.createdAt || this.pointer;
      this.storePointer();
      this.buffer = [];
    }

    await cursor.close();
  }

  async run() {
    setInterval(async () => {
      if (!this.locked) {
        await this.loop();
      } else {
        console.log("[Pursuer]", "Pursuer has been locked");
        this.pointer = undefined;
      }
    }, 1000);
  }
}

async function run() {
  if (!DB_URI || !SOURCE_COLLECTION || !TARGET_COLLECTION) return;

  const client = new MongoClient(DB_URI);

  const database = client.db();
  const source = database.collection(SOURCE_COLLECTION);
  const target = database.collection(TARGET_COLLECTION);

  if (process.argv.includes(IS_FULL_REINDEX) && !lockfile.checkSync(LOCKFILE)) {
    lockfile.lockSync(LOCKFILE);
    await fullReindex(source, target);
    lockfile.unlockSync(LOCKFILE);
    process.exit(0);
  } else {
    console.log("Start Pursuer");
    const pursuer = new Pursuer(source, target);
    await pursuer.run();

    console.log("Start Updater");
    const updater = new Updater(source, target);
    await updater.run();
  }
}

run().catch(console.debug);
