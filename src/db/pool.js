// src/db/pool.js
import sql from "mssql";
import { env } from "../config/env.js";

let pool;

export async function getPool() {
  if (pool) return pool;

  pool = await sql.connect({
    server: env.db.server,
    database: env.db.database,
    user: env.db.user,
    password: env.db.password,
    //requestTimeout: env.db.requestTimeout,
    options: {
      ...env.db.options,
      useUTC: false, // ✅ CLAVE: evita el +3
    },
    pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },

  // ⬇⬇⬇ CLAVE ⬇⬇⬇
  requestTimeout: 300000,        // 5 minutos
  connectionTimeout: 30000       // 30 segundos
  });

  return pool;
}

export { sql };
