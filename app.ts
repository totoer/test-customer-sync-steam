require("dotenv").config();

import * as dotenv from "dotenv";
dotenv.config();

import { randomInt } from "crypto";
import { MongoClient } from "mongodb";
import { faker } from "@faker-js/faker";
import { Customer } from "./declarations";

const DB_URI = process.env.DB_URI;
const SOURCE_COLLECTION = process.env.SOURCE_COLLECTION;

async function run() {
  if (!DB_URI || !SOURCE_COLLECTION) return;

  const client = new MongoClient(DB_URI);
  const database = client.db();
  const customers = database.collection(SOURCE_COLLECTION);

  setInterval(async () => {
    const items: Customer[] = [];
    let countOfRandomCustomers = randomInt(1, 10);

    while (countOfRandomCustomers > 0) {
      items.push({
        firstName: faker.name.firstName(),
        lastName: faker.name.lastName(),
        email: faker.internet.email(),
        address: {
          line1: "34801 Kurt Spur",
          line2: "Suite 028",
          postcode: faker.address.zipCode(),
          city: faker.address.city(),
          state: faker.address.state(),
          country: faker.address.countryCode(),
        },
        createdAt: faker.date.past(),
      });
      countOfRandomCustomers--;
    }
    console.log(items.length);
    await customers.insertMany(items);
  }, 200);
}

run().catch(console.debug);
