import { SnelDBClient } from "../src/client";
import type { NormalizedRecord } from "../src/types";
import {
  ConnectionError,
  AuthenticationError,
  CommandError
} from "../src/errors";

const BASE_URL = process.env.SNELDB_DEMO_URL ?? "ws://localhost:7171";
const ADMIN_USER = process.env.SNELDB_ADMIN_USER ?? "admin";
const ADMIN_KEY = process.env.SNELDB_ADMIN_KEY ?? "admin-key-123";

const logSection = (title: string) => {
  console.log("\n" + "=".repeat(80));
  console.log(title);
  console.log("=".repeat(80));
};

const quoteIfNeeded = (value: string): string => {
  return /^[A-Za-z0-9_-]+$/.test(value) ? value : `"${value}"`;
};

const buildCreateUserCommand = (userId: string, secretKey?: string, roles?: string[]): string => {
  const parts = [`CREATE USER ${userId}`];
  if (secretKey) {
    const key = /^[A-Za-z0-9_-]+$/.test(secretKey) ? secretKey : `"${secretKey}"`;
    parts.push(`WITH KEY ${key}`);
  }
  if (roles && roles.length) {
    const rolesList = roles.map((role) => `"${role}"`).join(", ");
    parts.push(`WITH ROLES [${rolesList}]`);
  }
  return parts.join(" ");
};

const buildDefineCommand = (eventType: string, fields: Record<string, string | string[]>): string => {
  const schema = JSON.stringify(fields);
  return `DEFINE ${eventType} FIELDS ${schema}`;
};

const buildGrantCommand = (userId: string, permissions: string[], eventTypes: string[]): string => {
  const perms = permissions.map((perm) => perm.toUpperCase()).join(", ");
  const events = eventTypes.map(quoteIfNeeded).join(", ");
  return `GRANT ${perms} ON ${events} TO ${userId}`;
};

const buildStoreCommand = (eventType: string, contextId: string, payload: unknown): string => {
  const context = quoteIfNeeded(contextId);
  return `STORE ${eventType} FOR ${context} PAYLOAD ${JSON.stringify(payload)}`;
};

const printRecords = (records: NormalizedRecord[], indent = "  "): void => {
  if (!records.length) {
    console.log(`${indent}(no data)`);
    return;
  }
  for (const record of records) {
    console.log(indent + JSON.stringify(record, null, 2).replace(/\n/g, `\n${indent}`));
  }
};

async function ensureServer(client: SnelDBClient): Promise<void> {
  process.stdout.write("→ Checking server availability... ");
  const result = await client.executeSafe("PING");
  if (!result.ok) {
    throw new ConnectionError(`Cannot reach ${BASE_URL}: ${result.error?.message}`);
  }
  console.log("OK");
}

async function main(): Promise<void> {
  logSection("SnelDB JavaScript Client: Real-World Scenario");
  console.log(`Connecting to ${BASE_URL} as admin (${ADMIN_USER}) via WebSocket`);

  const adminClient = new SnelDBClient({
    baseUrl: BASE_URL,
    userId: ADMIN_USER,
    secretKey: ADMIN_KEY,
    logLevel: "debug"
  });

  await ensureServer(adminClient);

  try {
    const token = await adminClient.authenticate();
    console.log(`→ Authenticated as admin (token: ${token.slice(0, 12)}...)`);
  } catch (error) {
    await adminClient.close();
    throw error;
  }

  const analystUserId = `analyst_${Date.now()}`;
  const analystSecret = `secret-${Math.random().toString(36).slice(2, 8)}`;

  logSection("Step 1: Create Read-Only Analyst User");
  await adminClient.execute(buildCreateUserCommand(analystUserId, analystSecret, ["read-only"]));
  console.log(`→ Created ${analystUserId} (secret: ${analystSecret})`);

  logSection("Step 2: Define Event Types");
  type EventSchema = {
    name: string;
    fields: Record<string, string | string[]>;
  };

  const eventTypes: EventSchema[] = [
    {
      name: "order_created",
      fields: {
        order_id: "int",
        customer_id: "int",
        amount: "float",
        currency: "string",
        status: "string",
        created_at: "datetime"
      }
    },
    {
      name: "payment_processed",
      fields: {
        payment_id: "int",
        order_id: "int",
        amount: "float",
        payment_method: "string",
        processed_at: "datetime"
      }
    },
    {
      name: "order_shipped",
      fields: {
        order_id: "int",
        tracking_number: "string",
        carrier: "string",
        shipped_at: "datetime"
      }
    }
  ];

  for (const eventType of eventTypes) {
    const defineResult = await adminClient.executeSafe(buildDefineCommand(eventType.name, eventType.fields));
    if (defineResult.ok) {
      console.log(`→ Defined ${eventType.name}`);
    } else if (
      defineResult.error &&
      defineResult.error.message.includes("already defined")
    ) {
      console.log(`→ ${eventType.name} already defined, skipping`);
    } else {
      throw defineResult.error;
    }
  }

  logSection("Step 3: Grant Read Permissions to Analyst");
  for (const eventType of eventTypes) {
    await adminClient.execute(buildGrantCommand(analystUserId, ["read"], [eventType.name]));
    console.log(`→ Granted READ on ${eventType.name}`);
  }

  logSection("Step 4: Store Sample Events");
  const orders = [
    {
      order_id: 1001,
      customer_id: 501,
      amount: 149.99,
      currency: "USD",
      status: "confirmed",
      created_at: "2025-01-15T10:30:00Z"
    },
    {
      order_id: 1002,
      customer_id: 502,
      amount: 79.5,
      currency: "EUR",
      status: "confirmed",
      created_at: "2025-01-15T11:15:00Z"
    },
    {
      order_id: 1003,
      customer_id: 501,
      amount: 299.99,
      currency: "USD",
      status: "pending",
      created_at: "2025-01-15T12:00:00Z"
    }
  ];

  for (const order of orders) {
    const contextId = `customer-${order.customer_id}`;
    await adminClient.execute(buildStoreCommand("order_created", contextId, order));
    console.log(`→ Stored order_created event for order ${order.order_id}`);
  }

  const payments = [
    {
      payment_id: 2001,
      order_id: 1001,
      amount: 149.99,
      payment_method: "credit_card",
      processed_at: "2025-01-15T10:35:00Z"
    },
    {
      payment_id: 2002,
      order_id: 1002,
      amount: 79.5,
      payment_method: "paypal",
      processed_at: "2025-01-15T11:20:00Z"
    }
  ];

  for (const payment of payments) {
    const order = orders.find((o) => o.order_id === payment.order_id);
    if (!order) continue;
    await adminClient.execute(buildStoreCommand("payment_processed", `customer-${order.customer_id}`, payment));
    console.log(`→ Stored payment_processed event ${payment.payment_id}`);
  }

  const shipments = [
    {
      order_id: 1001,
      tracking_number: "TRACK-1001",
      carrier: "ups",
      shipped_at: "2025-01-16T09:00:00Z"
    }
  ];

  for (const shipment of shipments) {
    const order = orders.find((o) => o.order_id === shipment.order_id);
    if (!order) continue;
    await adminClient.execute(buildStoreCommand("order_shipped", `customer-${order.customer_id}`, shipment));
    console.log(`→ Stored order_shipped event for order ${shipment.order_id}`);
  }

  logSection("Step 5: Analyst Queries via Read-Only Session");
  const analystClient = new SnelDBClient({
    baseUrl: BASE_URL,
    userId: analystUserId,
    secretKey: analystSecret,
    logLevel: "debug"
  });
  await analystClient.authenticate();
  console.log(`→ Analyst authenticated with temporary token`);

  const highValueOrders = await analystClient.execute(
    "QUERY order_created RETURN [\"order_id\", \"amount\", \"currency\", \"status\", \"created_at\"] WHERE amount >= 100.0 LIMIT 10"
  );
  console.log("High-value orders:");
  printRecords(highValueOrders);

  const customerReplay = await analystClient.execute(
    "REPLAY order_created FOR \"customer-501\" SINCE \"2025-01-15T00:00:00Z\""
  );
  console.log("\nReplay for customer-501:");
  printRecords(customerReplay);

  logSection("Demo Complete");
  await analystClient.close();
  await adminClient.close();
}

main().catch((error) => {
  if (error instanceof AuthenticationError || error instanceof CommandError || error instanceof ConnectionError) {
    console.error(`\nError: ${error.message}`);
  } else {
    console.error(error);
  }
  process.exit(1);
});
