import { getPool } from "../db/pool.js";
import { bracket, fullTable } from "../utils/sqlIdentifiers.js";

export async function ensureImportSchema(schema) {
  const pool = await getPool();
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='${schema.replace(/'/g, "''")}')
    EXEC('CREATE SCHEMA ${bracket(schema)}');
  `);
}

export async function ensureTableFromExcel({ schema, tableName, columns }) {
  const pool = await getPool();
  await ensureImportSchema(schema);

  const t = fullTable(schema, tableName);

  // Columnas del excel como NVARCHAR(MAX) para arrancar robusto
  const colsSql = columns.map((c) => `${bracket(c)} NVARCHAR(MAX) NULL`).join(",\n      ");

  const sqlCreate = `
IF OBJECT_ID(N'${schema}.${tableName}', 'U') IS NULL
BEGIN
  CREATE TABLE ${t} (
    __Id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    __ImportedAt DATETIME2(0) NULL,
    __SourceFileName NVARCHAR(260) NULL,
    __RowNumber INT NULL,
    __RowHash CHAR(64) NOT NULL,
    ${colsSql}
  );

  CREATE UNIQUE INDEX ${bracket(`UX_${tableName}_RowHash`)}
  ON ${t}(__RowHash)
  WITH (IGNORE_DUP_KEY = ON);
END
`;

  await pool.request().query(sqlCreate);
}
