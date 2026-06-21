/**
 * DB admin script — creates or drops the demo database.
 * Usage:
 *   node --experimental-strip-types db-admin.ts create
 *   node --experimental-strip-types db-admin.ts drop
 */

import pg from "pg";
const { Client } = pg;

const PG_ADMIN_URL = process.env.PG_ADMIN_URL ?? "postgres://datacat:datacat@localhost:55432/postgres";
const DB_NAME = process.env.DEMO_DB ?? "datacat_demo";

const action = process.argv[2];
if (action !== "create" && action !== "drop") {
  console.error("Usage: db-admin.ts <create|drop>");
  process.exit(1);
}

const client = new Client({ connectionString: PG_ADMIN_URL });
await client.connect();

try {
  if (action === "drop") {
    // Terminate existing connections first
    await client.query(`
      SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = $1 AND pid <> pg_backend_pid()
    `, [DB_NAME]);
    await client.query(`DROP DATABASE IF EXISTS "${DB_NAME}"`);
    console.log(`[db-admin] Database '${DB_NAME}' dropped`);
  } else {
    await client.query(`DROP DATABASE IF EXISTS "${DB_NAME}"`);
    await client.query(`CREATE DATABASE "${DB_NAME}"`);
    console.log(`[db-admin] Database '${DB_NAME}' created`);
  }
} finally {
  await client.end();
}
