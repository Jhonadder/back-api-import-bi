import dotenv from "dotenv";
dotenv.config();

export const env = {
  port: Number(process.env.PORT || 3001),
  db: {
    server: process.env.DB_SERVER,
    database: process.env.DB_DATABASE,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    options: {
      encrypt: String(process.env.DB_ENCRYPT || "false") === "true",
      trustServerCertificate: String(process.env.DB_TRUST_CERT || "true") === "true",
    },
  },
  jwt: {
    secret: process.env.JWT_SECRET,
    expiresIn: process.env.JWT_EXPIRES_IN || "8h",
  },
  upload: {
    maxFileSizeMb: Number(process.env.MAX_FILE_SIZE_MB || 0),
    dir: process.env.UPLOAD_DIR || "uploads",
  },
  import: {
    batchSize: Number(process.env.BATCH_SIZE || 2000),
    schema: process.env.SCHEMA_IMPORT || "importacionxls",
  },
};
