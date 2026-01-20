import { env } from "./config/env.js";
import { createApp } from "./app.js";
import { getPool } from "./db/pool.js";

async function main() {
  await getPool(); // valida conexión al iniciar
  const app = createApp();
  app.listen(env.port, () => {
    console.log(`API on http://localhost:${env.port}`);
  });
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
